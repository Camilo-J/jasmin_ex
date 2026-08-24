Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Routing.BillingTest do
  use ExUnit.Case, async: true

  @moduletag :admit

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
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
