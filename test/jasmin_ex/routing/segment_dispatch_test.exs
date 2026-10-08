Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Routing.SegmentDispatchTest.InjectedOps do
  @moduledoc false
  alias JasminEx.Routing.FileOps

  def fail_dir!(dir), do: :persistent_term.put({__MODULE__, dir}, true)
  def clear_dir!(dir), do: :persistent_term.erase({__MODULE__, dir})
  def mkdir_p(path), do: FileOps.mkdir_p(path)
  def chmod(path, mode), do: FileOps.chmod(path, mode)
  def read(path), do: FileOps.read(path)
  def fsync(path), do: FileOps.fsync(path)
  def rename(from, to), do: FileOps.rename(from, to)

  def write(path, data) do
    if :persistent_term.get({__MODULE__, Path.dirname(path)}, false),
      do: {:error, :eio},
      else: FileOps.write(path, data)
  end
end

defmodule JasminEx.Routing.SegmentDispatchTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.Tombstone
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.Router
  alias JasminEx.Routing.Snapshot
  alias JasminEx.Routing.State
  alias JasminEx.Routing.User

  @ops __MODULE__.InjectedOps

  test "all confirms hold queued charges at full price without ledger accepted", %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)

    assert {:ok, %Reservation{} = reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert is_reference(generation)
    assert reservation.ledger.count == 2
    assert user_money(router) == {300, 3}

    Enum.each(["gw-a", "gw-b"], fn gateway_id ->
      assert {:ok, _dispatch} =
               Routing.claim_segment_dispatch(router, "bill-1", generation, gateway_id)

      assert {:ok, _result} =
               Routing.record_segment_dispatch(
                 router,
                 "bill-1",
                 generation,
                 gateway_id,
                 :queued
               )
    end)

    dispatch = snapshot_dispatch(router, "bill-1")
    assert Enum.map(dispatch.children, & &1.status) == [:queued, :queued]
    refute Enum.any?(dispatch.children, &(&1.status == :accepted))
    assert snapshot_dispatch(router, "bill-1").phase in [:dispatching, :closed]
    assert user_money(router) == {300, 3}
    assert Routing.snapshot(router).reservations["bill-1"].ledger.outcomes == %{}
  end

  test "definitive rejection at each position refunds the failing unit and suffix", %{
    tmp_dir: dir
  } do
    Enum.each([1, 2, 3], fn position ->
      {router, admission, children} =
        start_plan!(dir,
          segment_count: 3,
          submit_quota: 5,
          snapshot_name: "reject-#{position}.json",
          ids: ["gw-a", "gw-b", "gw-c"]
        )

      assert {:ok, _reservation, generation} =
               Routing.admit_segments_with_dispatch(router, admission, children)

      prefix = Enum.take(["gw-a", "gw-b", "gw-c"], position - 1)
      failing = Enum.at(["gw-a", "gw-b", "gw-c"], position - 1)

      Enum.each(prefix, fn gateway_id ->
        assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, gateway_id)

        assert {:ok, _} =
                 Routing.record_segment_dispatch(
                   router,
                   "bill-1",
                   generation,
                   gateway_id,
                   :queued
                 )
      end)

      assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, failing)

      assert {:ok, _result} =
               Routing.record_segment_dispatch(router, "bill-1", generation, failing, :rejected)

      queued = position - 1
      refunded = 3 - queued
      assert user_money(router) == {200 + refunded * 100, 2 + refunded}
      dispatch = snapshot_dispatch(router, "bill-1")
      assert dispatch.phase == :closed

      suffix = Enum.drop(["gw-a", "gw-b", "gw-c"], position)

      Enum.each(suffix, fn gateway_id ->
        assert {:error, _reason} =
                 Routing.claim_segment_dispatch(router, "bill-1", generation, gateway_id)
      end)
    end)
  end

  test "uncertain record holds the attempted unit and refunds only the suffix", %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)

    assert {:ok, _reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

    assert {:ok, _result} =
             Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :uncertain)

    assert user_money(router) == {400, 4}
    ledger = Routing.snapshot(router).reservations["bill-1"].ledger
    assert ledger.outcomes[1] == :uncertain
    assert ledger.outcomes[2] == :rejected
    assert snapshot_dispatch(router, "bill-1").phase == :closed

    assert {:error, _reason} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-b")
  end

  test "invalid plan does not debit, own, or persist a checkpoint", %{tmp_dir: dir} do
    {router, admission, _children} = start_plan!(dir, segment_count: 2, submit_quota: 5)
    before = Routing.snapshot(router)

    assert {:error, :invalid_gateway_id} =
             Routing.admit_segments_with_dispatch(router, admission, [
               %{gateway_id: "bill-1", payload_hash: hash(1)},
               %{gateway_id: "gw-b", payload_hash: hash(2)}
             ])

    assert Routing.snapshot(router) == before
    assert Routing.snapshot(router).reservations == %{}
    assert Routing.snapshot(router).segment_dispatches == %{}

    assert {:error, :generation_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", make_ref(), "gw-a")
  end

  test "duplicate admit does not debit, reset, or return a generation", %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)

    assert {:ok, _reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
    assert {:ok, :duplicate} = Routing.admit_segments_with_dispatch(router, admission, children)
    assert user_money(router) == {300, 3}
    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]
  end

  test "foreign and live owners cannot claim or recover", %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)
    parent = self()

    owner =
      spawn(fn ->
        {:ok, _reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        send(parent, {:generation, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:generation, generation}, 1_000

    assert {:error, :owner_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

    assert {:error, :owner_alive} = Routing.recover_segment_dispatch(router, "bill-1")
    assert statuses(snapshot_dispatch(router, "bill-1")) == [:unattempted, :unattempted]
    assert user_money(router) == {300, 3}
    assert Process.alive?(owner)
  end

  test "actor killed before the first claim refunds the whole unpublished plan", %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)
    parent = self()

    owner =
      spawn(fn ->
        {:ok, _reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        send(parent, {:generation, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:generation, generation}, 1_000
    await_down!(owner)
    _ = sync_recover(router, "bill-1")
    assert user_money(router) == {500, 5}
    assert snapshot_dispatch(router, "bill-1").phase == :closed

    assert {:error, :generation_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
  end

  test "actor killed after claim holds uncertain and refunds the suffix", %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)
    parent = self()

    owner =
      spawn(fn ->
        {:ok, _reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
        send(parent, {:generation, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:generation, generation}, 1_000
    await_down!(owner)
    _ = sync_recover(router, "bill-1")
    assert user_money(router) == {400, 4}
    ledger = Routing.snapshot(router).reservations["bill-1"].ledger
    assert ledger.outcomes[1] == :uncertain
    assert ledger.outcomes[2] == :rejected
    assert snapshot_dispatch(router, "bill-1").phase == :closed

    assert {:error, :generation_mismatch} =
             Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :queued)
  end

  test "actor killed after queued record keeps the prefix and refunds the suffix", %{
    tmp_dir: dir
  } do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)
    parent = self()

    owner =
      spawn(fn ->
        {:ok, _reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

        {:ok, _} =
          Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :queued)

        send(parent, {:generation, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:generation, _generation}, 1_000
    await_down!(owner)
    _ = sync_recover(router, "bill-1")
    assert user_money(router) == {400, 4}
    ledger = Routing.snapshot(router).reservations["bill-1"].ledger
    refute Map.has_key?(ledger.outcomes, 1)
    assert ledger.outcomes[2] == :rejected
    assert hd(snapshot_dispatch(router, "bill-1").children).status == :queued
  end

  test "router restart recovers without a live owner and rejects the old generation", %{
    tmp_dir: dir
  } do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5)

    assert {:ok, _reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
    router = restart_router!(router, config)
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert user_money(router) == {400, 4}

    assert {:error, :generation_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-b")

    assert {:error, :generation_mismatch} =
             Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :queued)
  end

  test "queued prefix accepted by SMPP is kept and suffix compensation does not double refund",
       %{tmp_dir: dir} do
    {router, admission, children} = start_plan!(dir, segment_count: 2, submit_quota: 5)
    parent = self()

    owner =
      spawn(fn ->
        {:ok, reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

        {:ok, _} =
          Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :queued)

        send(parent, {:ready, reservation.fingerprint})
        Process.sleep(:infinity)
      end)

    assert_receive {:ready, fingerprint}, 1_000

    assert {:ok, _} = Routing.settle_segment(router, "bill-1", fingerprint, 1, :accepted)

    await_down!(owner)
    _ = sync_recover(router, "bill-1")
    assert user_money(router) == {400, 4}
    assert %Tombstone{} = Routing.snapshot(router).tombstones["bill-1"]
    refute Map.has_key?(Routing.snapshot(router).reservations, "bill-1")
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {400, 4}
  end

  test "claimed child already accepted survives owner death and router boot without extra refund",
       %{tmp_dir: dir} do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5)

    parent = self()

    owner =
      spawn(fn ->
        {:ok, reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
        send(parent, {:ready, reservation.fingerprint, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:ready, fingerprint, generation}, 1_000
    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]
    assert {:ok, _} = Routing.settle_segment(router, "bill-1", fingerprint, 1, :accepted)
    assert user_money(router) == {300, 3}
    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]

    await_down!(owner)
    _ = sync_recover(router, "bill-1")
    assert user_money(router) == {400, 4}
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {400, 4}

    assert {:error, :generation_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-b")

    router = restart_router!(router, config)
    assert Process.alive?(router)
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert user_money(router) == {400, 4}
  end

  test "claimed child already rejected is not refunded twice across recover and boot", %{
    tmp_dir: dir
  } do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5)

    parent = self()

    owner =
      spawn(fn ->
        {:ok, reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
        send(parent, {:ready, reservation.fingerprint, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:ready, fingerprint, generation}, 1_000
    assert {:ok, _} = Routing.settle_segment(router, "bill-1", fingerprint, 1, :rejected)
    assert user_money(router) == {400, 4}
    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]

    await_down!(owner)
    _ = sync_recover(router, "bill-1")
    assert user_money(router) == {500, 5}
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {500, 5}

    assert {:error, :generation_mismatch} =
             Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :queued)

    router = restart_router!(router, config)
    assert Process.alive?(router)
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert user_money(router) == {500, 5}
  end

  test "router boot recovers a durable claimed checkpoint after concurrent accepted settlement",
       %{tmp_dir: dir} do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5)

    assert {:ok, reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

    assert {:ok, _} =
             Routing.settle_segment(router, "bill-1", reservation.fingerprint, 1, :accepted)

    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]
    router = restart_router!(router, config)
    assert Process.alive?(router)
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert user_money(router) == {400, 4}
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {400, 4}

    assert {:error, :generation_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-b")
  end

  test "recovery snapshot failure rolls back suffix credit after a claimed rejection", %{
    tmp_dir: dir
  } do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5, file_ops: @ops)

    parent = self()

    owner =
      spawn(fn ->
        {:ok, reservation, generation} =
          Routing.admit_segments_with_dispatch(router, admission, children)

        {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
        send(parent, {:ready, reservation.fingerprint, generation})
        Process.sleep(:infinity)
      end)

    assert_receive {:ready, fingerprint, generation}, 1_000
    assert {:ok, _} = Routing.settle_segment(router, "bill-1", fingerprint, 1, :rejected)
    assert user_money(router) == {400, 4}
    @ops.fail_dir!(Path.dirname(config.snapshot_path))
    on_exit(fn -> @ops.clear_dir!(Path.dirname(config.snapshot_path)) end)
    await_down!(owner)
    assert {:error, :snapshot_failed} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {400, 4}
    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]

    assert {:error, reason} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-b")

    assert reason in [:generation_mismatch, :owner_mismatch, :owner_fenced]

    @ops.clear_dir!(Path.dirname(config.snapshot_path))
    assert {:ok, _result} = Routing.recover_segment_dispatch(router, "bill-1")
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {500, 5}
    assert snapshot_dispatch(router, "bill-1").phase == :closed
  end

  test "admission snapshot failure leaves no owner and no debit", %{tmp_dir: dir} do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5, file_ops: @ops)

    before = Routing.snapshot(router)
    payload = File.read!(config.snapshot_path)
    @ops.fail_dir!(Path.dirname(config.snapshot_path))
    on_exit(fn -> @ops.clear_dir!(Path.dirname(config.snapshot_path)) end)

    assert {:error, :snapshot_failed} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert Routing.snapshot(router) == before
    assert File.read!(config.snapshot_path) == payload
    assert user_money(router) == {500, 5}

    assert {:error, :generation_mismatch} =
             Routing.claim_segment_dispatch(router, "bill-1", make_ref(), "gw-a")
  end

  test "claim snapshot failure does not change the checkpoint", %{tmp_dir: dir} do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5, file_ops: @ops)

    assert {:ok, _reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    @ops.fail_dir!(Path.dirname(config.snapshot_path))
    on_exit(fn -> @ops.clear_dir!(Path.dirname(config.snapshot_path)) end)

    assert {:error, :snapshot_failed} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

    assert statuses(snapshot_dispatch(router, "bill-1")) == [:unattempted, :unattempted]
    @ops.clear_dir!(Path.dirname(config.snapshot_path))

    assert {:ok, dispatch} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

    assert statuses(dispatch) == [:claimed, :unattempted]
  end

  test "record snapshot failure keeps the claim and recovery retry is idempotent", %{
    tmp_dir: dir
  } do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5, file_ops: @ops)

    assert {:ok, _reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")
    @ops.fail_dir!(Path.dirname(config.snapshot_path))
    on_exit(fn -> @ops.clear_dir!(Path.dirname(config.snapshot_path)) end)

    assert {:error, :snapshot_failed} =
             Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :queued)

    assert statuses(snapshot_dispatch(router, "bill-1")) == [:claimed, :unattempted]
    assert user_money(router) == {300, 3}

    assert {:error, :snapshot_failed} = Routing.recover_segment_dispatch(router, "bill-1")

    assert {:error, :owner_fenced} =
             Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-b")

    @ops.clear_dir!(Path.dirname(config.snapshot_path))
    assert {:ok, _result} = Routing.recover_segment_dispatch(router, "bill-1")
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {400, 4}
    assert snapshot_dispatch(router, "bill-1").phase == :closed
  end

  test "startup recovery failure stops init rather than exposing a claimable owner", %{
    tmp_dir: dir
  } do
    config =
      Config.new(
        snapshot_path: Path.join(dir, "boot.json"),
        file_ops: @ops,
        clock: clock()
      )

    {state, admission} = pure_fixture(segment_count: 2, submit_quota: 5)
    children = children_for(admission.bill, ["gw-a", "gw-b"])

    assert {:ok, admitted} =
             State.admit_segments_with_dispatch(state, admission, children, clock())

    assert :ok = Snapshot.write(admitted, config)
    @ops.fail_dir!(Path.dirname(config.snapshot_path))
    on_exit(fn -> @ops.clear_dir!(Path.dirname(config.snapshot_path)) end)
    assert {:error, :snapshot_failed} = GenServer.start(Router, config: config)
    @ops.clear_dir!(Path.dirname(config.snapshot_path))
    router = start_supervised!({Router, name: nil, config: config})
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert user_money(router) == {500, 5}
  end

  test "closed v6 checkpoints restore and standalone reservations are not unpublished", %{
    tmp_dir: dir
  } do
    {router, config, admission, children} =
      start_plan_config!(dir, segment_count: 2, submit_quota: 5)

    assert {:ok, _reservation, generation} =
             Routing.admit_segments_with_dispatch(router, admission, children)

    assert {:ok, _} = Routing.claim_segment_dispatch(router, "bill-1", generation, "gw-a")

    assert {:ok, _} =
             Routing.record_segment_dispatch(router, "bill-1", generation, "gw-a", :uncertain)

    router = restart_router!(router, config)
    assert snapshot_dispatch(router, "bill-1").phase == :closed
    assert user_money(router) == {400, 4}
    assert {:ok, :duplicate} = Routing.recover_segment_dispatch(router, "bill-1")
    assert user_money(router) == {400, 4}

    {legacy_router, _legacy_admission} =
      start_legacy!(dir, segment_count: 2, submit_quota: 5, snapshot_name: "legacy.json")

    before = user_money(legacy_router)
    assert Routing.snapshot(legacy_router).segment_dispatches == %{}
    assert {:error, :unknown_bill} = Routing.recover_segment_dispatch(legacy_router, "bill-1")
    assert user_money(legacy_router) == before
    assert Routing.snapshot(legacy_router).reservations["bill-1"].ledger.outcomes == %{}
  end

  defp start_plan!(dir, opts) do
    {router, _config, admission, children} = start_plan_config!(dir, opts)
    {router, admission, children}
  end

  defp start_plan_config!(dir, opts) do
    ids = Keyword.get(opts, :ids, ["gw-a", "gw-b"])
    {router, config, admission} = start_admitting_router(dir, opts)
    {router, config, admission, children_for(admission.bill, ids)}
  end

  defp start_legacy!(dir, opts) do
    {router, _config, admission} = start_admitting_router(dir, opts)
    assert {:ok, %Reservation{}} = Routing.admit_segments(router, admission)
    {router, admission}
  end

  defp start_admitting_router(tmp_dir, opts) do
    config =
      Config.new(
        snapshot_path: Path.join(tmp_dir, Keyword.get(opts, :snapshot_name, "routing.json")),
        file_ops: Keyword.get(opts, :file_ops),
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
        submit_quota: Keyword.get(opts, :submit_quota, 3)
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
        bill_id: Keyword.get(opts, :bill_id, "bill-1"),
        uid: "u1",
        route_order: 10,
        rate_minor: 100,
        precharge_percent: 10,
        segment_count: Keyword.get(opts, :segment_count, 1)
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1_000)
    {router, config, admission}
  end

  defp restart_router!(router, config) do
    ref = Process.monitor(router)
    Process.exit(router, :kill)
    assert_receive {:DOWN, ^ref, :process, ^router, _reason}, 1_000
    _ = stop_supervised({Router, config.snapshot_path})
    start_supervised!({Router, name: nil, config: config}, id: {Router, config.snapshot_path})
  end

  defp sync_recover(router, bill_id), do: Routing.recover_segment_dispatch(router, bill_id)

  defp await_down!(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
  end

  defp snapshot_dispatch(router, bill_id),
    do: Routing.snapshot(router).segment_dispatches[bill_id]

  defp user_money(router) do
    user = Routing.snapshot(router).users["u1"]
    {user.balance_minor, user.submit_quota}
  end

  defp statuses(dispatch), do: Enum.map(dispatch.children, & &1.status)

  defp children_for(%Bill{} = bill, ids) do
    Enum.with_index(ids, 1)
    |> Enum.map(fn {id, index} ->
      %{gateway_id: id, payload_hash: hash({bill.bill_id, index})}
    end)
  end

  defp hash(term), do: :crypto.hash(:sha256, :erlang.term_to_binary(term))

  defp clock, do: {FakeClock, FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)}

  defp pure_fixture(opts) do
    {:ok, group} = Group.new(gid: "ops")

    {:ok, user} =
      User.new(
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: 500,
        submit_quota: Keyword.get(opts, :submit_quota, 3)
      )

    {:ok, connector} = ConnectorRef.new("smpp-t")

    {:ok, route} =
      Route.new(
        kind: :static,
        order: 10,
        connector: connector,
        filters: [],
        rate_minor: 100,
        precharge_percent: 10
      )

    {:ok, state} = State.put_group(State.new(), group)
    {:ok, state} = State.put_user(state, user)
    {:ok, state} = State.put_route(state, route)

    {:ok, bill} =
      Bill.new(
        bill_id: "bill-1",
        uid: "u1",
        route_order: 10,
        rate_minor: 100,
        precharge_percent: 10,
        segment_count: Keyword.get(opts, :segment_count, 1)
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1_000)
    {state, admission}
  end
end
