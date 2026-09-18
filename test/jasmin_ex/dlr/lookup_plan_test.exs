defmodule JasminEx.Dlr.LookupPlanTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.{LookupPlan, Worker}
  alias JasminEx.Dlr.Map, as: DlrMap

  defmodule Clock do
    def now_ms(value), do: value
  end

  defmodule Store do
    def put(agent, key, value, ttl) do
      Agent.update(agent, fn state ->
        state
        |> Map.update!(:events, &[{:put, key} | &1])
        |> put_in([:entries, key], {value, ttl})
      end)

      :ok
    end

    def fetch(agent, key) do
      Agent.get(agent, fn state ->
        case state.entries[key] do
          {value, _ttl} -> {:ok, value}
          nil -> :missing
        end
      end)
    end

    def delete(agent, key) do
      Agent.get_and_update(agent, fn state ->
        event = {:delete, key}

        if Map.has_key?(state.entries, key) do
          {:deleted,
           %{state | entries: Map.delete(state.entries, key), events: [event | state.events]}}
        else
          {:missing, %{state | events: [event | state.events]}}
        end
      end)
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def clear(agent), do: Agent.update(agent, &%{&1 | events: []})
  end

  test "versioned plan codec round-trips bounded actions and phase" do
    plan = plan_fixture()

    assert {:ok, encoded} = LookupPlan.encode(plan)
    assert {:ok, decoded} = LookupPlan.decode(encoded)
    assert decoded == plan
    assert {:error, :unsupported_version} = LookupPlan.decode(~s({"version":2}))
    assert {:error, :invalid_plan} = LookupPlan.decode(~s({"version":1,"phase":"wat"}))
  end

  test "persists the plan before reverse mutation and publish, then cleans up after confirm" do
    {:ok, agent} = Agent.start_link(fn -> %{entries: %{}, events: []} end)
    store = {Store, agent}
    clock = {Clock, 1_000}
    :ok = DlrMap.register(store, request_fixture(%{level: 3}), clock)
    Store.clear(agent)

    publisher = fn job ->
      Agent.update(agent, &%{&1 | events: [{:publish, job.job_id} | &1.events]})
      :ok
    end

    assert :ok =
             LookupPlan.process_event(submit_event(),
               store: store,
               clock: clock,
               publisher: publisher
             )

    events = Store.events(agent)
    plan_key = LookupPlan.key("event-submit")
    reverse_key = DlrMap.reverse_key("c1", "AB12")

    assert [
             {:put, ^plan_key},
             {:put, ^reverse_key},
             {:publish, "event-submit"},
             {:put, ^plan_key},
             {:put, ^plan_key}
           ] = events

    assert {:ok, %{phase: :complete}} = LookupPlan.fetch(store, "event-submit", clock)
  end

  test "forwarded replay retries cleanup without publishing the callback again" do
    {:ok, agent} = Agent.start_link(fn -> %{entries: %{}, events: []} end)
    store = {Store, agent}
    clock = {Clock, 1_000}
    :ok = DlrMap.register(store, request_fixture(), clock)
    :ok = LookupPlan.persist(store, %{plan_fixture() | phase: :forwarded}, clock)
    Store.clear(agent)

    publisher = fn _job -> flunk("forwarded replay must not republish") end

    assert :ok =
             LookupPlan.process_event(submit_event(),
               store: store,
               clock: clock,
               publisher: publisher
             )

    refute Enum.any?(Store.events(agent), &match?({:publish, _}, &1))
    assert {:ok, %{phase: :complete}} = LookupPlan.fetch(store, "event-submit", clock)
  end

  test "publication failure preserves planned phase and asks broker to retry" do
    {:ok, agent} = Agent.start_link(fn -> %{entries: %{}, events: []} end)
    store = {Store, agent}
    clock = {Clock, 1_000}
    :ok = DlrMap.register(store, request_fixture(), clock)

    assert :retry =
             LookupPlan.process_event(submit_event(),
               store: store,
               clock: clock,
               publisher: fn _job -> {:error, :unroutable} end
             )

    assert {:ok, %{phase: :planned}} = LookupPlan.fetch(store, "event-submit", clock)
    assert {:ok, _request} = DlrMap.fetch_request(store, "G1", clock)
  end

  test "worker accepts a context-bearing processor tuple" do
    context = %{marker: make_ref()}

    assert :ok =
             Worker.invoke_processor(
               {__MODULE__, :processor, context},
               "payload",
               %{routing_key: "dlr.submit_sm_resp"}
             )

    assert_received {:processed, "payload", "dlr.submit_sm_resp", ^context}
  end

  def processor(payload, meta, context) do
    send(self(), {:processed, payload, meta.routing_key, context})
    :ok
  end

  defp plan_fixture do
    %{
      version: 1,
      event_id: "event-submit",
      phase: :planned,
      expires_at_ms: 80_000,
      reverse: nil,
      cleanup: {:request, "G1"},
      job: %{
        job_id: "event-submit",
        event_id: "event-submit",
        gateway_id: "G1",
        url: "https://example.com/dlr",
        method: "POST",
        level: 1,
        created_at_ms: 1_000,
        deadline_ms: 80_000,
        fields: %{
          "id" => "G1",
          "level" => "1",
          "message_status" => "ESME_ROK",
          "connector" => "c1"
        }
      }
    }
  end

  defp request_fixture(overrides \\ %{}) do
    Map.merge(
      %{
        gateway_id: "G1",
        connector_id: "c1",
        url: "https://example.com/dlr",
        level: 1,
        method: "POST",
        expiry_s: 86_400
      },
      overrides
    )
  end

  defp submit_event do
    %{
      kind: :submit_sm_resp,
      event_id: "event-submit",
      gateway_id: "G1",
      connector_id: "c1",
      status: "ESME_ROK",
      raw_smsc_id: "00ab12",
      observed_at_ms: 1_000,
      deadline_ms: 80_000
    }
  end
end
