Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Billing.ContractsTest do
  use ExUnit.Case, async: true

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.Settlement
  alias JasminEx.Billing.Tombstone

  @max_int64 9_223_372_036_854_775_807

  describe "command identity" do
    test "admission and open reservation share (bill_id, fingerprint, :admit)" do
      {bill, fingerprint, admission, reservation} = open_fixture(bill_id: "bill-a", ttl_ms: 500)
      assert Admission.identity(admission) == {bill.bill_id, fingerprint, :admit}
      assert Reservation.identity(reservation) == Admission.identity(admission)
    end

    test "settlement identity includes outcome and matches sealed tombstone semantics" do
      {_bill, fingerprint, _admission, reservation} = open_fixture(bill_id: "bill-b")
      assert {:ok, ok_stone} = Tombstone.seal(reservation, :settled_ok)
      assert {:ok, non_ok_stone} = Tombstone.seal(reservation, :settled_non_ok)
      assert {:ok, expired} = Tombstone.seal(reservation, :expired)
      assert {:ok, ok} = settle("bill-b", fingerprint, :ok)
      assert {:ok, non_ok} = settle("bill-b", fingerprint, :non_ok)
      assert Settlement.identity(ok) == {"bill-b", fingerprint, :ok}
      assert Settlement.identity(non_ok) == {"bill-b", fingerprint, :non_ok}
      assert Tombstone.identity(ok_stone) == Settlement.identity(ok)
      assert Tombstone.identity(non_ok_stone) == Settlement.identity(non_ok)
      assert Tombstone.identity(expired) == {"bill-b", fingerprint, :expired}
    end
  end

  describe "conflicting fingerprint and outcome stay typed unchanged" do
    test "same bill_id with a different fingerprint is billing_conflict" do
      {_bill, _fingerprint, _admission, reservation} =
        open_fixture(bill_id: "bill-c", rate_minor: 100)

      {_other, other_fp, other_admission, _other_reservation} =
        open_fixture(bill_id: "bill-c", rate_minor: 50)

      before = reservation
      assert Reservation.classify(reservation, other_admission) == {:error, :billing_conflict}
      assert reservation == before
      assert {:ok, stone} = Tombstone.seal(reservation, :settled_ok)
      stone_before = stone
      assert {:ok, other_settle} = settle("bill-c", other_fp, :ok)
      assert Tombstone.classify(stone, other_admission) == {:error, :billing_conflict}
      assert Tombstone.classify(stone, other_settle) == {:error, :billing_conflict}
      assert stone == stone_before
    end

    test "matching admission is a duplicate against open and terminal records" do
      {bill, fingerprint, admission, reservation} = open_fixture(bill_id: "bill-d")
      assert Reservation.classify(reservation, admission) == {:ok, :duplicate, bill, fingerprint}
      assert {:ok, stone} = Tombstone.seal(reservation, :expired)
      assert Tombstone.classify(stone, admission) == {:ok, :duplicate, bill, fingerprint}
    end

    test "matching settlement is a duplicate; opposite outcome conflicts; ACK after expiry is late" do
      {_bill, fingerprint, _admission, reservation} = open_fixture(bill_id: "bill-e")
      assert {:ok, ok_stone} = Tombstone.seal(reservation, :settled_ok)
      assert {:ok, non_ok_stone} = Tombstone.seal(reservation, :settled_non_ok)
      assert {:ok, expired} = Tombstone.seal(reservation, :expired)
      assert {:ok, ok} = settle("bill-e", fingerprint, :ok)
      assert {:ok, non_ok} = settle("bill-e", fingerprint, :non_ok)
      before = ok_stone
      assert Tombstone.classify(ok_stone, ok) == {:ok, :duplicate}
      assert Tombstone.classify(non_ok_stone, non_ok) == {:ok, :duplicate}
      assert Tombstone.classify(ok_stone, non_ok) == {:error, :conflicting_settlement}
      assert Tombstone.classify(non_ok_stone, ok) == {:error, :conflicting_settlement}
      assert Tombstone.classify(expired, ok) == {:ok, :late_ignored}
      assert Tombstone.classify(expired, non_ok) == {:ok, :late_ignored}
      assert ok_stone == before
    end
  end

  describe "permanent tombstone" do
    test "seal keeps only bill_id, fingerprint, and terminal state" do
      {_bill, fingerprint, _admission, reservation} = open_fixture(bill_id: "bill-f")
      assert {:ok, stone} = Tombstone.seal(reservation, :settled_ok)
      assert stone == %Tombstone{bill_id: "bill-f", fingerprint: fingerprint, state: :settled_ok}
      assert Enum.sort(Map.keys(Map.from_struct(stone))) == [:bill_id, :fingerprint, :state]
      refute function_exported?(Tombstone, :prune, 1)

      for state <- [:settled_ok, :settled_non_ok, :expired] do
        assert {:ok, sealed} = Tombstone.seal(reservation, state)
        assert sealed.state == state
        assert sealed.fingerprint == fingerprint
      end
    end
  end

  describe "transport-neutral settlement fixtures" do
    test "fixtures carry only bill_id, fingerprint, and outcome" do
      {_bill, fingerprint, _admission, _reservation} = open_fixture(bill_id: "bill-h")
      assert {:ok, ok} = settle("bill-h", fingerprint, :ok)
      assert {:ok, non_ok} = settle("bill-h", fingerprint, :non_ok)
      assert Map.from_struct(ok) == %{bill_id: "bill-h", fingerprint: fingerprint, outcome: :ok}

      assert non_ok.outcome == :non_ok and non_ok.bill_id == "bill-h" and
               non_ok.fingerprint == fingerprint

      refute function_exported?(Settlement, :publish, 1)
      assert settle("", fingerprint, :ok) == {:error, :invalid_bill_id}
      assert settle(:binary.copy("b", 129), fingerprint, :ok) == {:error, :invalid_bill_id}
    end
  end

  describe "admission and reservation constructors" do
    test "opens a reservation from the bill split, 0%, 100%, and injected clock" do
      clock = FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)
      assert {:ok, bill} = Bill.new(valid_attrs(rate_minor: 100, precharge_percent: 10))
      assert {:ok, fingerprint} = Fingerprint.compute(bill)
      assert {:ok, admission} = Admission.new(bill: bill, ttl_ms: 500)
      assert {:ok, reservation} = Reservation.open(admission, {FakeClock, clock})
      assert reservation.state == :open
      assert reservation.fingerprint == fingerprint

      assert {reservation.captured_minor, reservation.reserved_minor,
              reservation.refundable_minor} ==
               {10, 90, 90}

      assert {reservation.wall_deadline_ms, reservation.monotonic_deadline_ms} == {1_500, 510}

      zero_clock = FakeClock.new(wall_ms: 0, monotonic_ms: 0)
      assert {:ok, full_bill} = Bill.new(valid_attrs(rate_minor: 250, precharge_percent: 100))

      assert {:ok, zero_bill} =
               Bill.new(valid_attrs(bill_id: "bill-z", rate_minor: 250, precharge_percent: 0))

      assert {:ok, full} = Admission.new(bill: full_bill, ttl_ms: 1)
      assert {:ok, zero} = Admission.new(bill: zero_bill, ttl_ms: 1)
      assert {:ok, full_res} = Reservation.open(full, {FakeClock, zero_clock})
      assert {:ok, zero_res} = Reservation.open(zero, {FakeClock, zero_clock})

      assert {full_res.captured_minor, full_res.reserved_minor, full_res.refundable_minor} ==
               {250, 0, 0}

      assert {zero_res.captured_minor, zero_res.reserved_minor, zero_res.refundable_minor} ==
               {0, 250, 250}
    end

    test "rejects invalid TTL and overflow deadlines" do
      assert {:ok, bill} = Bill.new(valid_attrs())
      assert Admission.new(bill: bill, ttl_ms: -1) == {:error, :invalid_ttl}
      assert Admission.new(bill: bill, ttl_ms: @max_int64 + 1) == {:error, :invalid_ttl}
      clock = FakeClock.new(wall_ms: @max_int64, monotonic_ms: 0)
      assert {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1)
      assert Reservation.open(admission, {FakeClock, clock}) == {:error, :invalid_ttl}
    end
  end

  defp open_fixture(overrides) do
    ttl_ms = Keyword.get(overrides, :ttl_ms, 1_000)
    clock = FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)
    {:ok, bill} = Bill.new(valid_attrs(Keyword.drop(overrides, [:ttl_ms])))
    {:ok, fingerprint} = Fingerprint.compute(bill)
    {:ok, admission} = Admission.new(bill: bill, ttl_ms: ttl_ms)
    {:ok, reservation} = Reservation.open(admission, {FakeClock, clock})
    {bill, fingerprint, admission, reservation}
  end

  defp settle(bill_id, fingerprint, outcome) do
    Settlement.new(bill_id: bill_id, fingerprint: fingerprint, outcome: outcome)
  end

  defp valid_attrs(overrides \\ []) do
    [
      bill_id: "bill-1",
      uid: "user-1",
      route_order: 0,
      rate_minor: 100,
      precharge_percent: 10
    ]
    |> Keyword.merge(overrides)
  end
end
