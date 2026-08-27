Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Routing.BillingTest.InjectedOps do
  @moduledoc false
  alias JasminEx.Routing.FileOps

  def fail_dir!(dir), do: :persistent_term.put({__MODULE__, dir}, true)
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

defmodule JasminEx.Routing.BillingTest do
  use ExUnit.Case, async: true

  @moduletag :admit

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.Settlement
  alias JasminEx.Billing.Tombstone
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.Router
  alias JasminEx.Routing.Snapshot
  alias JasminEx.Routing.State
  alias JasminEx.Routing.User

  test "finite success reserves, captures, and decrements" do
    {state, admission} =
      fixture(balance_minor: 500, submit_quota: 3, rate_minor: 100, precharge_percent: 10)

    before = state

    assert {:ok, %State{} = next} = State.admit(state, admission, clock())
    user = next.users["u1"]
    reservation = next.reservations["bill-1"]

    assert user.balance_minor == 400
    assert user.submit_quota == 2
    assert reservation.state == :open
    assert reservation.captured_minor == 10
    assert reservation.reserved_minor == 90
    assert reservation.refundable_minor == 90
    assert reservation.wall_deadline_ms == 2_000
    assert reservation.monotonic_deadline_ms == 1_010
    assert before.users["u1"].balance_minor == 500
    assert before.reservations == %{}
  end

  test "insufficient balance leaves state unchanged" do
    {state, admission} = fixture(balance_minor: 99, rate_minor: 100)
    assert {:error, :insufficient_balance} = State.admit(state, admission, clock())
    assert state.users["u1"].balance_minor == 99
    assert state.reservations == %{}
  end

  test "insufficient quota leaves state unchanged" do
    {state, admission} = fixture(submit_quota: 0, rate_minor: 100)
    assert {:error, :insufficient_quota} = State.admit(state, admission, clock())
    assert state.users["u1"].submit_quota == 0
    assert state.users["u1"].balance_minor == 500
    assert state.reservations == %{}
  end

  test "unlimited nil balance and quota skip debit" do
    {state, admission} = fixture(balance_minor: nil, submit_quota: nil)
    assert {:ok, next} = State.admit(state, admission, clock())
    assert next.users["u1"].balance_minor == nil
    assert next.users["u1"].submit_quota == nil
    assert next.reservations["bill-1"].state == :open
    assert next.reservations["bill-1"].reserved_minor == 90
  end

  test "zero-percent pre-charge reserves full cost and captures 0" do
    {state, admission} = fixture(precharge_percent: 0, rate_minor: 250)
    assert {:ok, next} = State.admit(state, admission, clock())
    reservation = next.reservations["bill-1"]
    assert next.users["u1"].balance_minor == 250

    assert {reservation.captured_minor, reservation.reserved_minor, reservation.refundable_minor} ==
             {0, 250, 250}
  end

  test "full pre-charge captures full cost and reserves 0" do
    {state, admission} = fixture(precharge_percent: 100, rate_minor: 250)
    assert {:ok, next} = State.admit(state, admission, clock())
    reservation = next.reservations["bill-1"]
    assert next.users["u1"].balance_minor == 250

    assert {reservation.captured_minor, reservation.reserved_minor, reservation.refundable_minor} ==
             {250, 0, 0}
  end

  test "per-segment independence keeps the other bill_id unchanged" do
    {state, first} = fixture(balance_minor: 250, submit_quota: 2)
    assert {:ok, mid} = State.admit(state, first, clock())
    first_res = mid.reservations["bill-1"]
    second = admission(bill_id: "bill-2", rate_minor: 100, precharge_percent: 10)
    assert {:ok, next} = State.admit(mid, second, clock())
    assert next.reservations["bill-1"] == first_res
    assert next.reservations["bill-2"].state == :open
    assert next.users["u1"].balance_minor == 50
    assert next.users["u1"].submit_quota == 0
    assert map_size(next.reservations) == 2
  end

  test "unknown user and unknown route fail typed unchanged" do
    {state, _admission} = fixture()
    unknown_user = admission(uid: "missing")
    unknown_route = admission(route_order: 99)
    assert {:error, :unknown_user} = State.admit(state, unknown_user, clock())
    assert {:error, :unknown_route} = State.admit(state, unknown_route, clock())
    assert state.reservations == %{}
    assert state.users["u1"].balance_minor == 500
  end

  test "malformed admission is contained without mutating state" do
    {state, _admission} = fixture()
    assert {:error, :invalid_bill_id} = State.admit(state, :not_an_admission, clock())
    assert {:error, :invalid_bill_id} = State.admit(state, %{bill_id: "bill-1"}, clock())
    assert state.reservations == %{}
    assert state.revision == 0
  end

  describe "router admission" do
    @describetag :router_admit
    @describetag :tmp_dir

    test "persist-before-publish admits a finite bill and persists the next revision", %{
      tmp_dir: tmp_dir
    } do
      {router, config, admission} = start_admitting_router(tmp_dir)
      before = Routing.snapshot(router)

      assert {:ok, %Reservation{} = reservation} = Routing.admit(router, admission)
      assert reservation.state == :open
      assert reservation.captured_minor == 10
      assert reservation.reserved_minor == 90
      assert reservation.wall_deadline_ms == 2_000
      assert reservation.monotonic_deadline_ms == 1_010

      published = Routing.snapshot(router)
      assert published.revision == before.revision + 1
      assert published.users["u1"].balance_minor == 400
      assert published.users["u1"].submit_quota == 2
      assert published.reservations["bill-1"] == reservation
      assert File.exists?(config.snapshot_path)
      assert {:ok, restored} = Snapshot.restore(config)
      assert restored.revision == published.revision
      assert Process.alive?(router)
    end

    test "concurrent_same_id_admission serializes one mutation and a duplicate no-op", %{
      tmp_dir: tmp_dir
    } do
      {router, _config, admission} = start_admitting_router(tmp_dir)
      before = Routing.snapshot(router)

      results =
        1..2
        |> Enum.map(fn _ -> Task.async(fn -> Routing.admit(router, admission) end) end)
        |> Task.await_many()

      assert Enum.count(results, &match?({:ok, %Reservation{state: :open}}, &1)) == 1

      assert Enum.count(results, &match?({:ok, :duplicate, %Bill{}, %Fingerprint{}}, &1)) ==
               1

      published = Routing.snapshot(router)
      assert published.revision == before.revision + 1
      assert published.users["u1"].balance_minor == 400
      assert published.users["u1"].submit_quota == 2
      assert map_size(published.reservations) == 1
      assert Process.alive?(router)
    end

    test "malformed_bill_call_is_contained", %{tmp_dir: tmp_dir} do
      {router, config, admission} = start_admitting_router(tmp_dir)
      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)

      assert {:error, :invalid_bill_id} = Routing.admit(router, admission.bill)
      assert Process.alive?(router)
      assert Routing.snapshot(router) == before
      assert File.read!(config.snapshot_path) == payload
    end

    test "malformed_admission_call_is_contained", %{tmp_dir: tmp_dir} do
      {router, config, _admission} = start_admitting_router(tmp_dir)
      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)

      assert {:error, :invalid_bill_id} = Routing.admit(router, :not_an_admission)
      assert {:error, :invalid_bill_id} = Routing.admit(router, %{bill_id: "bill-1"})
      assert Process.alive?(router)
      assert Routing.snapshot(router) == before
      assert File.read!(config.snapshot_path) == payload
    end

    test "injected_snapshot_write_failure leaves published state and revision unchanged", %{
      tmp_dir: tmp_dir
    } do
      {router, config, admission} =
        start_admitting_router(tmp_dir, file_ops: __MODULE__.InjectedOps)

      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      __MODULE__.InjectedOps.fail_dir!(Path.dirname(config.snapshot_path))

      on_exit(fn ->
        :persistent_term.erase({__MODULE__.InjectedOps, Path.dirname(config.snapshot_path)})
      end)

      assert {:error, :snapshot_failed} = Routing.admit(router, admission)
      assert_router_unchanged(router, config, before, payload)
      assert Routing.snapshot(router).reservations == %{}
    end

    test "same bill_id with a different economic fingerprint is a live billing conflict", %{
      tmp_dir: tmp_dir
    } do
      {router, config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, %Reservation{}} = Routing.admit(router, admission)
      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)

      assert {:error, :billing_conflict} =
               Routing.admit(router, admission(rate_minor: 200))

      assert_router_unchanged(router, config, before, payload)
    end

    test "unknown_user is contained without mutating Router state", %{tmp_dir: tmp_dir} do
      assert_router_typed_error(tmp_dir, :unknown_user, admission(uid: "missing"))
    end

    test "unknown_route is contained without mutating Router state", %{tmp_dir: tmp_dir} do
      assert_router_typed_error(tmp_dir, :unknown_route, admission(route_order: 99))
    end

    test "insufficient_balance is contained without mutating Router state", %{tmp_dir: tmp_dir} do
      assert_router_typed_error(tmp_dir, :insufficient_balance, admission(rate_minor: 501))
    end

    test "Routing.admit/2 publishes a public admission contract" do
      {:docs_v1, _, :elixir, _, _, _, docs} = Code.fetch_docs(Routing)

      doc =
        Enum.find_value(docs, fn
          {{:function, :admit, 2}, _, _, %{"en" => text}, _} -> text
          _ -> nil
        end)

      assert is_binary(doc)
      assert doc =~ "persists"
      assert doc =~ "no-op"
      assert doc =~ "billing_conflict"
    end
  end

  describe "settlement" do
    @describetag :settle

    test "success ACK captures remainder into a settled_ok tombstone" do
      {state, admission} = fixture()
      assert {:ok, open} = State.admit(state, admission, clock())
      fingerprint = open.reservations["bill-1"].fingerprint
      assert {:ok, next} = State.settle(open, settle_cmd(admission, :ok))
      assert next.users["u1"].balance_minor == 400
      assert next.users["u1"].submit_quota == 2
      assert next.reservations == %{}

      assert next.tombstones["bill-1"] == %Tombstone{
               bill_id: "bill-1",
               fingerprint: fingerprint,
               state: :settled_ok
             }

      {full_state, full} = fixture(bill_id: "bill-full", rate_minor: 250, precharge_percent: 100)
      assert {:ok, full_open} = State.admit(full_state, full, clock())
      assert {:ok, full_next} = State.settle(full_open, settle_cmd(full, :ok))
      assert full_next.users["u1"].balance_minor == 250
      assert full_next.tombstones["bill-full"].state == :settled_ok
    end

    test "non-OK ACK keeps pre-charge and releases remainder" do
      {state, admission} = fixture()
      assert {:ok, open} = State.admit(state, admission, clock())
      assert {:ok, next} = State.settle(open, settle_cmd(admission, :non_ok))
      assert next.users["u1"].balance_minor == 490
      assert next.users["u1"].submit_quota == 2
      assert next.reservations == %{}
      assert next.tombstones["bill-1"].state == :settled_non_ok

      {zero_state, zero} = fixture(bill_id: "bill-z", rate_minor: 250, precharge_percent: 0)
      assert {:ok, zero_open} = State.admit(zero_state, zero, clock())
      assert {:ok, zero_next} = State.settle(zero_open, settle_cmd(zero, :non_ok))
      assert zero_next.users["u1"].balance_minor == 500

      {nil_state, unlimited} = fixture(bill_id: "bill-u", balance_minor: nil, submit_quota: nil)
      assert {:ok, nil_open} = State.admit(nil_state, unlimited, clock())
      assert {:ok, nil_next} = State.settle(nil_open, settle_cmd(unlimited, :non_ok))
      assert nil_next.users["u1"].balance_minor == nil
    end

    test "expiry releases remainder and empty sweep is a no-op" do
      {state, waiting} = fixture()
      assert {:ok, open} = State.admit(state, waiting, clock())
      before = open
      assert {:ok, ^before, 0} = State.expire_due(open, clock())

      {state, due} = fixture(ttl_ms: 0)
      assert {:ok, open} = State.admit(state, due, clock())
      other = admission(bill_id: "bill-2")
      assert {:ok, open} = State.admit(open, other, clock())
      assert {:ok, next, 1} = State.expire_due(open, clock())
      assert next.users["u1"].balance_minor == 390
      assert next.users["u1"].submit_quota == 1
      assert next.tombstones["bill-1"].state == :expired
      assert next.reservations["bill-2"].state == :open
      assert {:ok, ^next, 0} = State.expire_due(next, clock())

      later = {FakeClock, FakeClock.new(wall_ms: 3_000, monotonic_ms: 2_000)}
      assert {:ok, expired, 1} = State.expire_due(next, later)
      assert expired.users["u1"].balance_minor == 480
      assert expired.reservations == %{}
      assert expired.tombstones["bill-2"].state == :expired
    end

    test "late ACK, duplicates, and conflicting identity leave ledger unchanged" do
      {state, admission} = fixture(ttl_ms: 0)
      assert {:ok, open} = State.admit(state, admission, clock())
      assert {:ok, expired, 1} = State.expire_due(open, clock())
      before = expired
      assert {:ok, :late_ignored} = State.settle(expired, settle_cmd(admission, :ok))
      assert {:ok, :late_ignored} = State.settle(expired, settle_cmd(admission, :non_ok))
      assert expired == before

      {state, admission} = fixture()
      assert {:ok, open} = State.admit(state, admission, clock())
      assert {:ok, settled} = State.settle(open, settle_cmd(admission, :ok))
      before = settled
      assert {:ok, :duplicate} = State.settle(settled, settle_cmd(admission, :ok))

      assert {:error, :conflicting_settlement} =
               State.settle(settled, settle_cmd(admission, :non_ok))

      assert {:error, :billing_conflict} =
               State.settle(settled, settle_cmd(admission(rate_minor: 200), :ok))

      assert {:error, :unknown_bill} = State.settle(state, settle_cmd(admission, :ok))
      assert {:error, :invalid_bill_id} = State.settle(settled, :not_a_settlement)
      assert settled == before
    end

    test "malformed settlement outcome leaves ledger unchanged" do
      {state, admission} = fixture()
      assert {:ok, open} = State.admit(state, admission, clock())
      before = open
      malformed = %{settle_cmd(admission, :ok) | outcome: :malformed}
      assert {:error, :invalid_bill_id} = State.settle(open, malformed)
      assert {:error, :invalid_bill_id} = State.settle(open, %{malformed | outcome: :expired})
      assert open == before
      assert open.users["u1"].balance_minor == 400
      assert open.reservations["bill-1"].state == :open
      assert open.tombstones == %{}
    end

    test "retry after expiry uses a new bill_id and never prunes tombstones" do
      {state, admission} = fixture(ttl_ms: 0, submit_quota: 2)
      assert {:ok, open} = State.admit(state, admission, clock())
      assert {:ok, expired, 1} = State.expire_due(open, clock())
      stone = expired.tombstones["bill-1"]
      retry = admission(bill_id: "bill-2")
      assert {:ok, next} = State.admit(expired, retry, clock())
      assert next.tombstones["bill-1"] == stone
      assert next.reservations["bill-2"].state == :open
      assert {:ok, settled} = State.settle(next, settle_cmd(retry, :ok))
      assert settled.tombstones["bill-1"] == stone
      assert settled.tombstones["bill-2"].state == :settled_ok
      assert map_size(settled.tombstones) == 2
    end
  end

  describe "router settlement" do
    @describetag :settle
    @describetag :tmp_dir

    test "persist-before-publish settlement and expiry", %{tmp_dir: tmp_dir} do
      {router, config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, _} = Routing.admit(router, admission)
      after_admit = Routing.snapshot(router)

      assert {:ok, %Tombstone{state: :settled_ok}} =
               Routing.settle(router, settle_cmd(admission, :ok))

      published = Routing.snapshot(router)
      assert published.revision == after_admit.revision + 1
      assert published.reservations == %{}
      assert published.tombstones["bill-1"].state == :settled_ok
      assert published.users["u1"].balance_minor == 400
      assert {:ok, restored} = Snapshot.restore(config)
      assert restored.revision == published.revision

      due = admission(bill_id: "bill-due", ttl_ms: 0)
      assert {:ok, _} = Routing.admit(router, due)
      after_due = Routing.snapshot(router)
      assert {:ok, 1} = Routing.expire_due(router)
      expired = Routing.snapshot(router)
      assert expired.revision == after_due.revision + 1
      assert expired.tombstones["bill-due"].state == :expired
      assert expired.users["u1"].balance_minor == 390
      assert Process.alive?(router)
    end

    test "restart_duplicate", %{tmp_dir: tmp_dir} do
      {router, config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, _} = Routing.admit(router, admission)
      :ok = stop_supervised(Router)
      router = start_supervised!({Router, config: config})
      assert {:ok, :duplicate, %Bill{}, %Fingerprint{}} = Routing.admit(router, admission)

      assert {:ok, %Tombstone{state: :settled_ok}} =
               Routing.settle(router, settle_cmd(admission, :ok))

      :ok = stop_supervised(Router)
      router = start_supervised!({Router, config: config})
      assert {:ok, :duplicate} = Routing.settle(router, settle_cmd(admission, :ok))
      assert Routing.snapshot(router).tombstones["bill-1"].state == :settled_ok
      assert Process.alive?(router)
    end

    test "restart_expiry", %{tmp_dir: tmp_dir} do
      {router, config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, _} = Routing.admit(router, admission)
      :ok = stop_supervised(Router)
      due = %{config | clock: {FakeClock, FakeClock.new(wall_ms: 2_000, monotonic_ms: 1_010)}}
      router = start_supervised!({Router, config: due})
      assert {:ok, 1} = Routing.expire_due(router)
      expired = Routing.snapshot(router)
      assert expired.reservations == %{}
      assert expired.tombstones["bill-1"].state == :expired
      assert expired.users["u1"].balance_minor == 490
      assert Process.alive?(router)
    end

    test "malformed_settlement_call_is_contained", %{tmp_dir: tmp_dir} do
      {router, config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, _} = Routing.admit(router, admission)
      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      assert {:error, :invalid_bill_id} = Routing.settle(router, :not_a_settlement)
      assert {:error, :invalid_bill_id} = Routing.settle(router, admission)

      malformed = %{settle_cmd(admission, :ok) | outcome: :malformed}
      assert {:error, :invalid_bill_id} = Routing.settle(router, malformed)
      assert_router_unchanged(router, config, before, payload)
    end

    test "duplicates, late ACK, empty sweep, and errors do not write", %{tmp_dir: tmp_dir} do
      {router, config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, _} = Routing.admit(router, admission)
      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      assert {:ok, 0} = Routing.expire_due(router)
      assert_router_unchanged(router, config, before, payload)

      assert {:ok, %Tombstone{}} = Routing.settle(router, settle_cmd(admission, :ok))
      after_settle = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      assert {:ok, :duplicate} = Routing.settle(router, settle_cmd(admission, :ok))

      assert {:error, :conflicting_settlement} =
               Routing.settle(router, settle_cmd(admission, :non_ok))

      assert {:ok, :duplicate, %Bill{}, %Fingerprint{}} = Routing.admit(router, admission)
      assert_router_unchanged(router, config, after_settle, payload)

      due = admission(bill_id: "bill-due", ttl_ms: 0)
      assert {:ok, _} = Routing.admit(router, due)
      assert {:ok, 1} = Routing.expire_due(router)
      after_exp = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      assert {:ok, :late_ignored} = Routing.settle(router, settle_cmd(due, :ok))

      assert {:error, :unknown_bill} =
               Routing.settle(router, settle_cmd(admission(bill_id: "missing"), :ok))

      assert_router_unchanged(router, config, after_exp, payload)
    end
  end

  describe "administration" do
    @describetag :admin
    @describetag :tmp_dir

    test "administration mutation persists balance, quota, and rate", %{tmp_dir: tmp_dir} do
      {router, config, _admission} = start_admitting_router(tmp_dir)
      before = Routing.snapshot(router)

      assert {:ok, %User{balance_minor: 900}} = Routing.set_balance(router, "u1", 900)
      assert {:ok, %User{submit_quota: 12}} = Routing.set_quota(router, "u1", 12)
      assert {:ok, %Route{rate_minor: 250}} = Routing.set_rate(router, 10, 250)

      published = Routing.snapshot(router)
      assert published.revision == before.revision + 3
      assert published.users["u1"].balance_minor == 900
      assert published.users["u1"].submit_quota == 12
      assert published.routes.routes[10].rate_minor == 250
      assert {:ok, restored} = Snapshot.restore(config)
      assert restored.revision == published.revision
      assert restored.users["u1"].balance_minor == 900
      assert restored.users["u1"].submit_quota == 12
      assert restored.routes.routes[10].rate_minor == 250
      assert Process.alive?(router)
    end

    test "unlimited nil and unchanged admin values do not write on no-op", %{tmp_dir: tmp_dir} do
      {router, config, _admission} = start_admitting_router(tmp_dir)
      assert {:ok, %User{balance_minor: nil}} = Routing.set_balance(router, "u1", nil)
      assert {:ok, %User{submit_quota: nil}} = Routing.set_quota(router, "u1", nil)
      published = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      assert {:ok, %User{balance_minor: nil}} = Routing.set_balance(router, "u1", nil)
      assert {:ok, %User{submit_quota: nil}} = Routing.set_quota(router, "u1", nil)
      assert {:ok, %Route{rate_minor: 100}} = Routing.set_rate(router, 10, 100)
      assert_router_unchanged(router, config, published, payload)
    end

    test "malformed_admin_call_is_contained", %{tmp_dir: tmp_dir} do
      {router, config, _admission} = start_admitting_router(tmp_dir)
      before = Routing.snapshot(router)
      payload = File.read!(config.snapshot_path)
      overflow = 9_223_372_036_854_775_808

      assert {:error, :unknown_user} = Routing.set_balance(router, :not_a_uid, 1)
      assert {:error, :invalid_amount} = Routing.set_balance(router, "u1", :nope)
      assert {:error, :invalid_amount} = Routing.set_balance(router, "u1", -1)
      assert {:error, :amount_overflow} = Routing.set_balance(router, "u1", overflow)
      assert {:error, :unknown_user} = Routing.set_quota(router, "missing", 1)
      assert {:error, :unknown_route} = Routing.set_rate(router, :nope, 1)
      assert {:error, :unknown_route} = Routing.set_rate(router, 99, 1)
      assert {:error, :invalid_amount} = Routing.set_rate(router, 10, :nope)
      assert {:error, :invalid_amount} = Routing.set_rate(router, 10, -1)
      assert {:error, :amount_overflow} = Routing.set_rate(router, 10, overflow)
      assert {:error, :invalid_amount} = GenServer.call(router, {:set_balance, "u1"})
      assert {:error, :invalid_amount} = GenServer.call(router, {:set_quota, "u1"})
      assert {:error, :invalid_amount} = GenServer.call(router, {:set_rate, 10})
      assert_router_unchanged(router, config, before, payload)
    end

    test "connector-only success has no production transport side effect", %{tmp_dir: tmp_dir} do
      {router, _config, admission} = start_admitting_router(tmp_dir)
      assert {:ok, _} = Routing.set_rate(router, 10, 50)
      assert {:ok, _} = Routing.admit(router, admission)
      snap = Routing.snapshot(router)
      user = snap.users["u1"]
      group = snap.groups["ops"]

      assert {:ok, routable} =
               Routable.new(
                 user: user,
                 group: group,
                 source: "1616",
                 destination: "21200000",
                 content: "hello",
                 tags: []
               )

      assert {:ok, %ConnectorRef{id: "smpp-t"} = resolved} = Routing.resolve(router, routable)
      assert resolved == snap.routes.routes[10].connector
      refute is_map_key(Map.from_struct(resolved), :rate_minor)
      refute Process.whereis(JasminEx.Messaging.RabbitMQ.Supervisor)
      refute Process.whereis(JasminEx.Messaging.RabbitMQ.Connection)
      assert Process.alive?(router)
    end
  end

  test "rejects invalid user balance, quota, and route rate fields unchanged" do
    {:ok, group} = Group.new(gid: "ops")
    {:ok, connector} = ConnectorRef.new("smpp-t")
    overflow = 9_223_372_036_854_775_808

    assert {:error, :invalid_amount} =
             User.new(
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               balance_minor: -1
             )

    assert {:error, :amount_overflow} =
             User.new(
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               submit_quota: overflow
             )

    assert {:error, :invalid_percentage} =
             Route.new(
               kind: :static,
               order: 10,
               connector: connector,
               filters: [],
               precharge_percent: 101
             )

    assert {:error, :invalid_amount} =
             Route.new(
               kind: :static,
               order: 10,
               connector: connector,
               filters: [],
               rate_minor: -1
             )

    assert {:error, :amount_overflow} =
             Route.new(
               kind: :static,
               order: 10,
               connector: connector,
               filters: [],
               rate_minor: overflow
             )
  end

  defp clock, do: {FakeClock, FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)}

  defp start_admitting_router(tmp_dir, opts \\ []) do
    config =
      Config.new(
        snapshot_path: Path.join(tmp_dir, "routing-v1.json"),
        file_ops: Keyword.get(opts, :file_ops),
        clock: clock()
      )

    router = start_supervised!({Router, name: nil, config: config})
    {:ok, group} = Routing.put_group(router, gid: "ops")

    {:ok, _} =
      Routing.put_user(router,
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: 500,
        submit_quota: 3
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

    {router, config, admission([])}
  end

  defp assert_router_typed_error(tmp_dir, reason, admission) do
    {router, config, _seed} = start_admitting_router(tmp_dir)
    before = Routing.snapshot(router)
    payload = File.read!(config.snapshot_path)

    assert {:error, ^reason} = Routing.admit(router, admission)
    assert_router_unchanged(router, config, before, payload)
  end

  defp assert_router_unchanged(router, config, before, payload) do
    assert Process.alive?(router)
    assert Routing.snapshot(router) == before
    assert Routing.snapshot(router).revision == before.revision
    assert File.read!(config.snapshot_path) == payload
  end

  defp settle_cmd(admission, outcome) do
    {:ok, fingerprint} = Fingerprint.compute(admission.bill)

    {:ok, settlement} =
      Settlement.new(
        bill_id: admission.bill.bill_id,
        fingerprint: fingerprint,
        outcome: outcome
      )

    settlement
  end

  defp admission(opts) do
    {:ok, bill} =
      Bill.new(
        bill_id: Keyword.get(opts, :bill_id, "bill-1"),
        uid: Keyword.get(opts, :uid, "u1"),
        route_order: Keyword.get(opts, :route_order, 10),
        rate_minor: Keyword.get(opts, :rate_minor, 100),
        precharge_percent: Keyword.get(opts, :precharge_percent, 10)
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: Keyword.get(opts, :ttl_ms, 1_000))
    admission
  end

  defp fixture(opts \\ []) do
    {:ok, group} = Group.new(gid: "ops")

    {:ok, user} =
      User.new(
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: Keyword.get(opts, :balance_minor, 500),
        submit_quota: Keyword.get(opts, :submit_quota, 3)
      )

    {:ok, connector} = ConnectorRef.new("smpp-t")

    {:ok, route} =
      Route.new(
        kind: :static,
        order: 10,
        connector: connector,
        filters: [],
        rate_minor: Keyword.get(opts, :rate_minor, 100),
        precharge_percent: Keyword.get(opts, :precharge_percent, 10)
      )

    {:ok, state} = State.put_group(State.new(), group)
    {:ok, state} = State.put_user(state, user)
    {:ok, state} = State.put_route(state, route)

    {:ok, bill} =
      Bill.new(
        bill_id: Keyword.get(opts, :bill_id, "bill-1"),
        uid: "u1",
        route_order: 10,
        rate_minor: route.rate_minor,
        precharge_percent: route.precharge_percent
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: Keyword.get(opts, :ttl_ms, 1_000))
    {state, admission}
  end
end
