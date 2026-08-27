defmodule JasminEx.Routing.BillingTelemetryTest do
  use ExUnit.Case, async: false

  @moduletag :admin
  @moduletag :tmp_dir

  alias JasminEx.Routing
  alias JasminEx.Routing.{Config, ConnectorRef, Router, Telemetry, User}

  @events for suffix <- [[:billing], [:mutation], [:snapshot]],
              do: [:jasmin_ex, :routing | suffix]

  @forbidden [
    :bill_id,
    :fingerprint,
    :balance,
    :balance_minor,
    :quota,
    :submit_quota,
    :rate,
    :rate_minor,
    :precharge_percent,
    :precharge_minor,
    :remainder_minor,
    :captured_minor,
    :reserved_minor,
    :refundable_minor,
    :amount,
    :route_order,
    :route_id,
    :user_id,
    :uid,
    :username
  ]

  setup do
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(id, @events, &__MODULE__.handle_telemetry/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  def handle_telemetry(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end

  test "billing emit keeps aggregate counts/bytes and drops economic identity" do
    Telemetry.emit(
      [:billing],
      %{
        count: 2,
        bytes: 16,
        bill_id: "bill-1",
        balance_minor: 500,
        rate_minor: 100,
        fingerprint: "abc"
      },
      %{
        outcome: :ok,
        bill_id: "bill-1",
        uid: "u1",
        username: "alice",
        amount: 50,
        submit_quota: 3,
        route_order: 10
      }
    )

    {measurements, metadata} = event([:billing])
    assert measurements == %{count: 2, bytes: 16}
    assert metadata == %{}
    refute_economic_identity(measurements)
    refute_economic_identity(metadata)
  end

  test "billing emit allowlists integer aggregates and strips nested or string-key identity" do
    Telemetry.emit(
      [:billing],
      %{count: %{uid: "u1", amount: 900}, bytes: "bill-1"},
      %{
        connector: %{id: "smpp-t"},
        outcome: %{bill_id: "bill-1", fingerprint: "secret"},
        economic: %{balance_minor: 900}
      }
    )

    {nested_measurements, nested_metadata} = event([:billing])
    assert nested_measurements == %{}
    assert nested_metadata == %{}

    Telemetry.emit(
      [:billing],
      %{count: 1, bytes: 2},
      %{"amount" => 42, "bill_id" => "bill-string-key", "connector_id" => "smpp-t", outcome: :ok}
    )

    {string_key_measurements, string_key_metadata} = event([:billing])
    assert string_key_measurements == %{count: 1, bytes: 2}
    assert string_key_metadata == %{}
  end

  test "admin mutation telemetry exposes aggregate counts/bytes only", %{tmp_dir: tmp_dir} do
    router = start_router(tmp_dir)
    flush_events()
    assert {:ok, %User{balance_minor: 900}} = Routing.set_balance(router, "u1", 900)
    {measurements, metadata} = event([:billing])
    assert Map.keys(measurements) -- [:count, :bytes] == []
    assert is_integer(measurements.count) and measurements.count >= 1
    assert is_integer(Map.get(measurements, :bytes, 0))
    assert metadata == %{}
    refute_economic_identity(measurements)
    refute_economic_identity(metadata)
    refute inspect({measurements, metadata}) =~ "u1"
    refute inspect({measurements, metadata}) =~ "900"
  end

  defp event(suffix) do
    assert_receive {:telemetry, [:jasmin_ex, :routing | ^suffix], measurements, metadata}
    {measurements, metadata}
  end

  defp flush_events do
    receive do
      {:telemetry, _, _, _} -> flush_events()
    after
      0 -> :ok
    end
  end

  defp refute_economic_identity(map) do
    refute Enum.any?(@forbidden, &Map.has_key?(map, &1))
  end

  defp start_router(tmp_dir) do
    config = Config.new(snapshot_path: Path.join(tmp_dir, "routing-v1.json"))
    router = start_supervised!({Router, config: config})
    {:ok, group} = Routing.put_group(router, gid: "ops")

    {:ok, _} =
      Routing.put_user(router, uid: "u1", username: "alice", secret: "s3cret", group: group)

    {:ok, connector} = ConnectorRef.new("smpp-t")

    {:ok, _} =
      Routing.put_route(router,
        kind: :static,
        order: 10,
        connector: connector,
        filters: [],
        rate_minor: 100,
        precharge_percent: 10
      )

    router
  end
end
