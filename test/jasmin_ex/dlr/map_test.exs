defmodule JasminEx.Dlr.MapTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Map, as: DlrMap

  defmodule Clock do
    def now_ms(ms) when is_integer(ms), do: ms
    def now_ms(agent) when is_pid(agent), do: Agent.get(agent, & &1)
  end

  defmodule MemoryStore do
    def put(table, key, value, ttl_ms) do
      :ets.insert(table, {key, value, ttl_ms})
      :ok
    end

    def fetch(table, key) do
      case :ets.lookup(table, key) do
        [{^key, value, _ttl_ms}] -> {:ok, value}
        [] -> :missing
      end
    end

    def delete(table, key) do
      case :ets.take(table, key) do
        [{^key, _value, _ttl_ms}] -> :deleted
        [] -> :missing
      end
    end

    def ttl(table, key) do
      case :ets.lookup(table, key) do
        [{^key, _value, ttl_ms}] -> ttl_ms
        [] -> :missing
      end
    end
  end

  defmodule ScriptedStore do
    def put(agent, key, value, ttl_ms) do
      Agent.get_and_update(agent, fn state ->
        case state.put do
          [{result, :write} | rest] ->
            {result, put_value(%{state | put: rest}, key, value, ttl_ms)}

          [{result, :keep} | rest] ->
            {result, %{state | put: rest}}

          [] ->
            {:ok, put_value(state, key, value, ttl_ms)}
        end
      end)
    end

    def fetch(agent, key) do
      Agent.get(agent, fn state ->
        case Map.fetch(state.entries, key) do
          {:ok, {value, _ttl_ms}} -> {:ok, value}
          :error -> :missing
        end
      end)
    end

    def delete(agent, key) do
      Agent.get_and_update(agent, fn state ->
        if Map.has_key?(state.entries, key) do
          {:deleted, %{state | entries: Map.delete(state.entries, key)}}
        else
          {:missing, state}
        end
      end)
    end

    def seed(agent, key, value, ttl_ms \\ 86_400_000) do
      Agent.update(agent, fn state ->
        %{state | entries: Map.put(state.entries, key, {value, ttl_ms})}
      end)
    end

    def entry(agent, key) do
      Agent.get(agent, fn state -> Map.get(state.entries, key) end)
    end

    defp put_value(state, key, value, ttl_ms) do
      %{state | entries: Map.put(state.entries, key, {value, ttl_ms})}
    end
  end

  setup do
    table = :ets.new(:dlr_map_store, [:set, :public])
    %{store: {MemoryStore, table}, table: table}
  end

  describe "request map" do
    test "valid HTTP request map is readable", %{store: store} do
      assert :ok = DlrMap.register(store, request_attrs(), {Clock, 1_000})
      assert {:ok, record} = DlrMap.fetch_request(store, "G1", {Clock, 1_000})
      assert record.gateway_id == "G1"
      assert record.source == "httpapi"
      assert record.connector_id == "c1"
      assert record.url == "http://example.com/dlr"
      assert record.level == 1
      assert record.method == "POST"
      assert record.expiry_s == 86_400
      assert record.created_at_ms == 1_000
      assert record.expires_at_ms == 1_000 + 86_400_000
    end

    test "unknown version is malformed not missing", %{store: store, table: table} do
      assert :ok = DlrMap.register(store, request_attrs(), {Clock, 0})
      [{key, value, ttl}] = :ets.tab2list(table)
      tampered = value |> :json.decode() |> Map.put("version", 99) |> encode()
      :ets.insert(table, {key, tampered, ttl})

      assert {:error, {:malformed_map, :unsupported_version}} =
               DlrMap.fetch_request(store, "G1", {Clock, 0})
    end

    test "absent key is missing", %{store: store} do
      assert :missing = DlrMap.fetch_request(store, "G-missing", {Clock, 0})
    end

    test "logical expiry is missing even if physically present", %{store: store, table: table} do
      assert :ok = DlrMap.register(store, request_attrs(%{expiry_s: 10}), {Clock, 0})
      assert {:ok, _record} = DlrMap.fetch_request(store, "G1", {Clock, 9_999})
      assert :missing = DlrMap.fetch_request(store, "G1", {Clock, 10_000})
      assert [{_key, _value, _ttl}] = :ets.tab2list(table)
    end

    test "source other than httpapi is malformed", %{store: store, table: table} do
      assert :ok = DlrMap.register(store, request_attrs(), {Clock, 0})
      [{key, value, ttl}] = :ets.tab2list(table)
      tampered = value |> :json.decode() |> Map.put("source", "smpps") |> encode()
      :ets.insert(table, {key, tampered, ttl})

      assert {:error, {:malformed_map, :invalid_source}} =
               DlrMap.fetch_request(store, "G1", {Clock, 0})
    end

    test "seconds-to-ms TTL is applied and is not refreshed", %{store: store, table: table} do
      assert :ok = DlrMap.register(store, request_attrs(%{expiry_s: 10}), {Clock, 1_000})
      assert [{_key, _value, 10_000}] = :ets.tab2list(table)
      assert {:ok, record} = DlrMap.fetch_request(store, "G1", {Clock, 1_000})
      assert record.expires_at_ms == 11_000
      assert :ok = DlrMap.register(store, request_attrs(%{expiry_s: 10}), {Clock, 1_000})
      assert {:ok, again} = DlrMap.fetch_request(store, "G1", {Clock, 4_000})
      assert again.expires_at_ms == 11_000
    end

    test "delete removes a request map", %{store: store} do
      assert :ok = DlrMap.register(store, request_attrs(), {Clock, 0})
      assert :deleted = DlrMap.delete_request(store, "G1")
      assert :missing = DlrMap.fetch_request(store, "G1", {Clock, 0})
    end

    test "gateway IDs are matched exactly without SMSC normalization", %{store: store} do
      assert :ok = DlrMap.register(store, request_attrs(%{gateway_id: "0abc"}), {Clock, 0})
      assert {:ok, record} = DlrMap.fetch_request(store, "0abc", {Clock, 0})
      assert record.gateway_id == "0abc"
      assert :missing = DlrMap.fetch_request(store, "ABC", {Clock, 0})
    end
  end

  describe "reverse map" do
    test "normalizes SMSC IDs and starts a new connector expiry", %{store: store, table: table} do
      assert :ok =
               DlrMap.put_reverse(
                 store,
                 reverse_attrs(%{raw_smsc_id: "00ab12", expiry_s: 86_400}),
                 {Clock, 5_000}
               )

      assert {:ok, record} = DlrMap.fetch_reverse(store, "c1", "00ab12", {Clock, 5_000})
      assert record.normalized_smsc_id == "AB12"
      assert record.raw_smsc_id == "00ab12"
      assert record.gateway_id == "G1"
      assert record.source == "httpapi"
      assert record.expires_at_ms == 5_000 + 86_400_000
      assert [{_key, _value, 86_400_000}] = :ets.tab2list(table)
    end

    test "all-zero nonempty raw ID normalizes to an empty component", %{store: store} do
      assert :ok = DlrMap.put_reverse(store, reverse_attrs(%{raw_smsc_id: "0000"}), {Clock, 0})
      assert {:ok, record} = DlrMap.fetch_reverse(store, "c1", "0000", {Clock, 0})
      assert record.normalized_smsc_id == ""
      assert {:ok, ^record} = DlrMap.fetch_reverse(store, "c1", "00", {Clock, 0})
    end

    test "empty raw ID is invalid", %{store: store} do
      assert {:error, :invalid_smsc_id} =
               DlrMap.put_reverse(store, reverse_attrs(%{raw_smsc_id: ""}), {Clock, 0})
    end

    test "two connectors sharing an SMSC ID do not collide", %{store: store} do
      assert :ok = DlrMap.put_reverse(store, reverse_attrs(%{connector_id: "c1"}), {Clock, 0})

      assert :ok =
               DlrMap.put_reverse(
                 store,
                 reverse_attrs(%{connector_id: "c2", gateway_id: "G2"}),
                 {Clock, 0}
               )

      assert {:ok, first} = DlrMap.fetch_reverse(store, "c1", "00ab12", {Clock, 0})
      assert {:ok, second} = DlrMap.fetch_reverse(store, "c2", "00ab12", {Clock, 0})
      assert first.gateway_id == "G1"
      assert second.gateway_id == "G2"
    end

    test "reverse expiry does not inherit remaining request TTL", %{store: store} do
      assert :ok = DlrMap.register(store, request_attrs(%{expiry_s: 100}), {Clock, 0})
      assert {:ok, request} = DlrMap.fetch_request(store, "G1", {Clock, 0})
      assert request.expires_at_ms == 100_000

      assert :ok =
               DlrMap.put_reverse(store, reverse_attrs(%{expiry_s: 86_400}), {Clock, 50_000})

      assert {:ok, reverse} = DlrMap.fetch_reverse(store, "c1", "00ab12", {Clock, 50_000})
      assert reverse.expires_at_ms == 50_000 + 86_400_000
      refute reverse.expires_at_ms == request.expires_at_ms
    end
  end

  describe "reverse collision policy" do
    test "identical mapping is idempotent and a different live mapping is terminal", %{
      store: store,
      table: table
    } do
      assert :ok = DlrMap.put_reverse(store, reverse_attrs(), {Clock, 0})
      before = :ets.tab2list(table)
      assert :ok = DlrMap.put_reverse(store, reverse_attrs(), {Clock, 1_000})
      assert :ets.tab2list(table) == before

      assert {:error, :reverse_collision} =
               DlrMap.put_reverse(store, reverse_attrs(%{gateway_id: "G-other"}), {Clock, 0})

      assert :ets.tab2list(table) == before
    end

    test "malformed reverse value is terminal and is never overwritten" do
      {:ok, agent} = Agent.start_link(fn -> %{entries: %{}, put: []} end)
      store = {ScriptedStore, agent}
      key = DlrMap.reverse_key("c1", "AB12")
      ScriptedStore.seed(agent, key, "not-json")

      assert {:error, {:malformed_map, _reason}} =
               DlrMap.put_reverse(store, reverse_attrs(), {Clock, 0})

      assert {"not-json", _} = ScriptedStore.entry(agent, key)
    end

    test "ambiguous write is read back and compared before success" do
      {:ok, agent} =
        Agent.start_link(fn -> %{entries: %{}, put: [{{:ambiguous, :timeout}, :write}]} end)

      store = {ScriptedStore, agent}

      assert :ok = DlrMap.put_reverse(store, reverse_attrs(), {Clock, 0})
      assert {:ok, record} = DlrMap.fetch_reverse(store, "c1", "00ab12", {Clock, 0})
      assert record.gateway_id == "G1"
    end

    test "ambiguous write that reads back a different mapping is terminal" do
      {:ok, agent} =
        Agent.start_link(fn -> %{entries: %{}, put: [{{:ambiguous, :timeout}, :keep}]} end)

      store = {ScriptedStore, agent}
      key = DlrMap.reverse_key("c1", "AB12")

      other =
        %{
          "version" => 1,
          "kind" => "reverse",
          "connector_id" => "c1",
          "raw_smsc_id" => "00ab12",
          "normalized_smsc_id" => "AB12",
          "gateway_id" => "G-other",
          "source" => "httpapi",
          "created_at_ms" => 0,
          "expires_at_ms" => 86_400_000
        }
        |> encode()

      ScriptedStore.seed(agent, key, other)

      assert {:error, :reverse_collision} =
               DlrMap.put_reverse(store, reverse_attrs(), {Clock, 0})
    end
  end

  defp request_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        gateway_id: "G1",
        url: "http://example.com/dlr",
        level: 1,
        method: "POST",
        connector_id: "c1",
        expiry_s: 86_400
      },
      overrides
    )
  end

  defp reverse_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        connector_id: "c1",
        raw_smsc_id: "00ab12",
        gateway_id: "G1",
        expiry_s: 86_400
      },
      overrides
    )
  end

  defp encode(map), do: map |> :json.encode() |> IO.iodata_to_binary()
end
