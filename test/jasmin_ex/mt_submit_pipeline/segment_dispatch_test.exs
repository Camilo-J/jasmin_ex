Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.MtSubmitPipeline.SegmentDispatchTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Messaging.Envelope
  alias JasminEx.MtSubmitPipeline.SegmentDispatch, as: Driver
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Router

  defmodule ScriptedQueue do
    @moduledoc false

    def start(scripts) do
      {:ok, pid} = Agent.start_link(fn -> %{scripts: scripts, published: []} end)
      pid
    end

    def enqueue(agent, envelope) do
      agent
      |> Agent.get_and_update(fn state ->
        case state.scripts do
          [next | rest] ->
            {next, %{state | scripts: rest, published: state.published ++ [envelope]}}

          [] ->
            {:ok, %{state | published: state.published ++ [envelope]}}
        end
      end)
      |> unwrap()
    end

    def published(agent), do: Agent.get(agent, & &1.published)

    defp unwrap(fun) when is_function(fun, 0), do: fun.()
    defp unwrap({:raise, message}), do: raise(message)
    defp unwrap({:exit, reason}), do: exit(reason)
    defp unwrap(result), do: result
  end

  test "all broker confirms queue sequentially and hold full price", %{tmp_dir: dir} do
    {router, admission, envelopes} = start_plan!(dir, 2)
    queue = {ScriptedQueue, ScriptedQueue.start([:ok, :ok])}

    assert {:ok, "bill-1"} = Driver.run(router, queue, admission, envelopes)
    published = ScriptedQueue.published(elem(queue, 1))
    assert Enum.map(published, & &1.gateway_id) == ["gw-a", "gw-b"]
    assert Enum.all?(published, &(&1.segment.count == 2))
    assert user_money(router) == {300, 3}
    dispatch = Routing.snapshot(router).segment_dispatches["bill-1"]
    assert Enum.map(dispatch.children, & &1.status) == [:queued, :queued]
    assert Routing.snapshot(router).reservations["bill-1"].ledger.outcomes == %{}
  end

  test "definitive rejection stops and does not publish the suffix", %{tmp_dir: dir} do
    {router, admission, envelopes} = start_plan!(dir, 3)
    queue = {ScriptedQueue, ScriptedQueue.start([:ok, {:error, :non_ok}, :ok])}

    assert {:error, :non_ok} = Driver.run(router, queue, admission, envelopes)
    assert Enum.map(ScriptedQueue.published(elem(queue, 1)), & &1.gateway_id) == ["gw-a", "gw-b"]
    assert user_money(router) == {400, 4}
    dispatch = Routing.snapshot(router).segment_dispatches["bill-1"]
    assert dispatch.phase == :closed
    assert Enum.map(dispatch.children, & &1.status) == [:queued, :failed, :unattempted]
  end

  test "generic, malformed, raise, and exit outcomes hold the attempted unit", %{tmp_dir: dir} do
    Enum.each(
      [
        {:error, :channel_closed},
        {:ambiguous, :channel_closed},
        :malformed,
        {:raise, "enqueue boom"},
        {:exit, :boom}
      ],
      fn script ->
        {router, admission, envelopes} =
          start_plan!(dir, 2, snapshot_name: "hold-#{:erlang.phash2(script)}.json")

        queue = {ScriptedQueue, ScriptedQueue.start([script, :ok])}
        assert {:error, _reason} = Driver.run(router, queue, admission, envelopes)
        assert Enum.map(ScriptedQueue.published(elem(queue, 1)), & &1.gateway_id) == ["gw-a"]
        assert user_money(router) == {400, 4}
        ledger = Routing.snapshot(router).reservations["bill-1"].ledger
        assert ledger.outcomes[1] == :uncertain
        assert ledger.outcomes[2] == :rejected
      end
    )
  end

  test "invalid envelopes never admit or enqueue", %{tmp_dir: dir} do
    {router, admission, envelopes} = start_plan!(dir, 2)
    queue = {ScriptedQueue, ScriptedQueue.start([:ok, :ok])}
    before = Routing.snapshot(router)
    bad = List.replace_at(envelopes, 1, List.first(envelopes))
    assert {:error, _reason} = Driver.run(router, queue, admission, bad)
    assert ScriptedQueue.published(elem(queue, 1)) == []
    assert Routing.snapshot(router) == before
    assert user_money(router) == {500, 5}
  end

  test "duplicate driver runs do not debit or republish", %{tmp_dir: dir} do
    {router, admission, envelopes} = start_plan!(dir, 2)
    queue = {ScriptedQueue, ScriptedQueue.start([:ok, :ok])}
    assert {:ok, "bill-1"} = Driver.run(router, queue, admission, envelopes)
    again = {ScriptedQueue, ScriptedQueue.start([:ok, :ok])}
    assert {:ok, :duplicate} = Driver.run(router, again, admission, envelopes)
    assert ScriptedQueue.published(elem(again, 1)) == []
    assert user_money(router) == {300, 3}
  end

  test "claim is persisted before the queue side effect", %{tmp_dir: dir} do
    {router, admission, envelopes} = start_plan!(dir, 2)
    parent = self()

    queue =
      {ScriptedQueue,
       ScriptedQueue.start([
         fn ->
           send(parent, {:claimed, statuses(router)})
           :ok
         end,
         :ok
       ])}

    assert {:ok, "bill-1"} = Driver.run(router, queue, admission, envelopes)
    assert_receive {:claimed, statuses}, 1_000
    assert statuses == [:claimed, :unattempted]
  end

  test "router death after a durable claim does not enqueue the suffix", %{tmp_dir: dir} do
    {router, config, admission, envelopes} = start_plan_config!(dir, 2)
    parent = self()

    queue =
      {ScriptedQueue,
       ScriptedQueue.start([
         fn ->
           send(parent, :first)
           ref = Process.monitor(router)
           Process.exit(router, :kill)

           receive do
             {:DOWN, ^ref, :process, ^router, _} -> :ok
           after
             1_000 -> :ok
           end
         end,
         fn ->
           send(parent, :second)
           :ok
         end
       ])}

    owner =
      spawn(fn ->
        send(parent, {:result, Driver.run(router, queue, admission, envelopes)})
      end)

    owner_ref = Process.monitor(owner)
    assert_receive :first, 1_000
    assert_receive {:result, {:error, _reason}}, 1_000
    refute_receive :second, 100
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, _reason}, 1_000
    restarted = restart_router!(router, config)
    assert Routing.snapshot(restarted).segment_dispatches["bill-1"].phase == :closed
    refute Enum.any?(ScriptedQueue.published(elem(queue, 1)), &(&1.gateway_id == "gw-b"))
  end

  defp start_plan!(dir, count, opts \\ []) do
    {router, _config, admission, envelopes} = start_plan_config!(dir, count, opts)
    {router, admission, envelopes}
  end

  defp start_plan_config!(dir, count, opts \\ []) do
    config =
      Config.new(
        snapshot_path: Path.join(dir, Keyword.get(opts, :snapshot_name, "routing.json")),
        clock: clock()
      )

    router =
      start_supervised!({Router, name: nil, config: config}, id: {Router, config.snapshot_path})

    {:ok, group} = Routing.put_group(router, gid: "ops")

    {:ok, _} =
      Routing.put_user(router,
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: 500,
        submit_quota: 5
      )

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

    {:ok, bill} =
      Bill.new(
        bill_id: "bill-1",
        uid: "u1",
        route_order: 10,
        rate_minor: 100,
        precharge_percent: 10,
        segment_count: count
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1_000)
    {:ok, fingerprint} = Fingerprint.compute(bill)
    ids = Enum.map(1..count, fn n -> "gw-" <> <<96 + n>> end)

    envelopes =
      Enum.with_index(ids, 1)
      |> Enum.map(fn {gateway_id, index} ->
        envelope!(bill, fingerprint, gateway_id, index, count)
      end)

    {router, config, admission, envelopes}
  end

  defp restart_router!(router, config) do
    ref = Process.monitor(router)
    Process.exit(router, :kill)
    assert_receive {:DOWN, ^ref, :process, ^router, _reason}, 1_000
    _ = stop_supervised({Router, config.snapshot_path})
    start_supervised!({Router, name: nil, config: config}, id: {Router, config.snapshot_path})
  end

  defp envelope!(bill, fingerprint, gateway_id, index, count) do
    {:ok, envelope} =
      Envelope.new(%{
        gateway_id: gateway_id,
        connector_id: "smpp-t",
        attempt: 1,
        max_attempts: 3,
        enqueued_at: "2026-08-01T15:00:00Z",
        expires_at: "2026-08-02T15:00:00Z",
        submit_sm: %{
          source_addr: "+12025550100",
          destination_addr: "+12025550101",
          short_message: "seg-#{index}"
        },
        segment: %{
          bill_id: bill.bill_id,
          index: index,
          count: count,
          fingerprint_version: 1,
          fingerprint_digest_base64: Base.encode64(fingerprint.digest)
        }
      })

    envelope
  end

  defp statuses(router) do
    Enum.map(Routing.snapshot(router).segment_dispatches["bill-1"].children, & &1.status)
  end

  defp user_money(router) do
    user = Routing.snapshot(router).users["u1"]
    {user.balance_minor, user.submit_quota}
  end

  defp clock, do: {FakeClock, FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)}
end
