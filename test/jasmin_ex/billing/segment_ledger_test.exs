defmodule JasminEx.Billing.SegmentLedgerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.SegmentLedger

  @max_int64 9_223_372_036_854_775_807
  @zero_delta %{refund_minor: 0, quota_credit: 0}

  describe "open/1" do
    test "binds bill_id and recomputed fingerprint for N=1 and N=3" do
      {:ok, one} = Bill.new(valid_attrs())
      {:ok, one_fp} = Fingerprint.compute(one)
      assert {:ok, one_ledger} = SegmentLedger.open(one)
      assert one_ledger.bill_id == one.bill_id
      assert one_ledger.fingerprint == one_fp
      refute Map.has_key?(Map.from_struct(one_ledger), :attempt)
      refute function_exported?(SegmentLedger, :record, 6)

      {:ok, three} = Bill.new(valid_attrs(rate_minor: 3, precharge_percent: 50, segment_count: 3))
      {:ok, three_fp} = Fingerprint.compute(three)
      assert {:ok, three_ledger} = SegmentLedger.open(three)
      assert three.quota_debit == 3
      assert three_ledger.bill_id == three.bill_id
      assert three_ledger.fingerprint == three_fp
    end

    test "rejects non-Bill input and forged bills without raising" do
      {:ok, bill} = Bill.new(valid_attrs(rate_minor: 3, precharge_percent: 50, segment_count: 3))

      assert SegmentLedger.open(:not_a_bill) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{}) == {:error, :invalid_bill}
      assert SegmentLedger.open(nil) == {:error, :invalid_bill}
      assert SegmentLedger.open("bill") == {:error, :invalid_bill}

      assert SegmentLedger.open(%{bill | bill_id: ""}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | uid: :binary.copy("u", 129)}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | route_order: -1}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | rate_minor: -9}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | precharge_minor: 1.0}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | precharge_percent: 101}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | quota_debit: 0}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | quota_debit: 256}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | quota_debit: 1.0}) == {:error, :invalid_bill}
      assert SegmentLedger.open(%{bill | remainder_minor: 0}) == {:error, :invalid_bill}

      swapped = %{bill | precharge_minor: 6, remainder_minor: 3}
      assert swapped.precharge_minor + swapped.remainder_minor == swapped.rate_minor
      assert SegmentLedger.open(swapped) == {:error, :invalid_bill}

      assert SegmentLedger.open(%{bill | precharge_minor: 4, remainder_minor: 5}) ==
               {:error, :indivisible_bill}

      assert SegmentLedger.open(%{bill | rate_minor: 10, remainder_minor: 7}) ==
               {:error, :indivisible_bill}
    end

    test "accepts zero-price N=7, protocol bound 255, and int64 amount boundary" do
      {:ok, zero} = Bill.new(valid_attrs(rate_minor: 0, precharge_percent: 50, segment_count: 7))
      assert {:ok, zero_ledger} = SegmentLedger.open(zero)
      assert zero.quota_debit == 7

      {:ok, max_n} =
        Bill.new(valid_attrs(rate_minor: 2, precharge_percent: 10, segment_count: 255))

      assert {:ok, max_ledger} = SegmentLedger.open(max_n)
      assert max_n.quota_debit == 255

      {:ok, max_amount} = Bill.new(valid_attrs(rate_minor: @max_int64, precharge_percent: 100))
      assert {:ok, _ledger} = SegmentLedger.open(max_amount)

      max_unit = div(@max_int64, 255)

      {:ok, max_n_amount} =
        Bill.new(valid_attrs(rate_minor: max_unit, precharge_percent: 0, segment_count: 255))

      assert {:ok, _} = SegmentLedger.open(max_n_amount)
      assert max_n_amount.rate_minor == max_unit * 255

      assert SegmentLedger.record(
               zero_ledger,
               zero.bill_id,
               zero_ledger.fingerprint,
               8,
               :rejected
             ) ==
               {:error, :invalid_index}

      assert SegmentLedger.record(
               max_ledger,
               max_n.bill_id,
               max_ledger.fingerprint,
               256,
               :rejected
             ) ==
               {:error, :invalid_index}
    end
  end

  describe "record/5" do
    test "first reject of unit 3 at 50% N=3 refunds 3 not 2; duplicate is zero" do
      {bill, fingerprint, ledger} =
        open_ledger(rate_minor: 3, precharge_percent: 50, segment_count: 3)

      assert {bill.rate_minor, bill.precharge_minor, bill.remainder_minor} == {9, 3, 6}

      assert {:ok, rejected, %{refund_minor: 3, quota_credit: 1}} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :rejected)

      refute match?(
               {:ok, _, %{refund_minor: 2, quota_credit: _}},
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :rejected)
             )

      assert {:ok, :duplicate, ^rejected, @zero_delta} =
               SegmentLedger.record(rejected, bill.bill_id, fingerprint, 1, :rejected)
    end

    test "terminal accepted and rejected cannot change and leave state unchanged" do
      {bill, fingerprint, ledger} = open_ledger(segment_count: 3)

      assert {:ok, rejected, %{refund_minor: 100, quota_credit: 1}} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :rejected)

      before_rejected = rejected

      assert SegmentLedger.record(rejected, bill.bill_id, fingerprint, 1, :accepted) ==
               {:error, :conflicting_settlement}

      assert SegmentLedger.record(rejected, bill.bill_id, fingerprint, 1, :uncertain) ==
               {:error, :conflicting_settlement}

      assert rejected == before_rejected

      assert {:ok, accepted, @zero_delta} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :accepted)

      before_accepted = accepted

      assert SegmentLedger.record(accepted, bill.bill_id, fingerprint, 1, :rejected) ==
               {:error, :conflicting_settlement}

      assert SegmentLedger.record(accepted, bill.bill_id, fingerprint, 1, :uncertain) ==
               {:error, :conflicting_settlement}

      assert accepted == before_accepted
    end

    test "uncertain may resolve once; repeats are duplicate zeros" do
      {bill, fingerprint, ledger} =
        open_ledger(rate_minor: 3, precharge_percent: 50, segment_count: 3)

      assert {:ok, pending, @zero_delta} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 2, :uncertain)

      assert {:ok, :duplicate, ^pending, @zero_delta} =
               SegmentLedger.record(pending, bill.bill_id, fingerprint, 2, :uncertain)

      assert {:ok, rejected, %{refund_minor: 3, quota_credit: 1}} =
               SegmentLedger.record(pending, bill.bill_id, fingerprint, 2, :rejected)

      assert SegmentLedger.record(rejected, bill.bill_id, fingerprint, 2, :accepted) ==
               {:error, :conflicting_settlement}

      assert {:ok, accepted_from_uncertain, @zero_delta} =
               SegmentLedger.record(pending, bill.bill_id, fingerprint, 2, :accepted)

      assert {:ok, :duplicate, ^accepted_from_uncertain, @zero_delta} =
               SegmentLedger.record(
                 accepted_from_uncertain,
                 bill.bill_id,
                 fingerprint,
                 2,
                 :accepted
               )
    end

    test "distinct segments are independent and threaded totals stay within the bill" do
      {bill, fingerprint, ledger} =
        open_ledger(rate_minor: 3, precharge_percent: 50, segment_count: 3)

      assert {:ok, a1, %{refund_minor: 0, quota_credit: 0}} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 2, :accepted)

      assert {:ok, a2, %{refund_minor: 3, quota_credit: 1}} =
               SegmentLedger.record(a1, bill.bill_id, fingerprint, 1, :rejected)

      assert {:ok, a3, @zero_delta} =
               SegmentLedger.record(a2, bill.bill_id, fingerprint, 3, :uncertain)

      assert {:ok, b1, %{refund_minor: 3, quota_credit: 1}} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :rejected)

      assert {:ok, b2, @zero_delta} =
               SegmentLedger.record(b1, bill.bill_id, fingerprint, 3, :uncertain)

      assert {:ok, b3, @zero_delta} =
               SegmentLedger.record(b2, bill.bill_id, fingerprint, 2, :accepted)

      assert {:ok, _a4, %{refund_minor: 3, quota_credit: 1}} =
               SegmentLedger.record(a3, bill.bill_id, fingerprint, 3, :rejected)

      assert {:ok, _b4, %{refund_minor: 3, quota_credit: 1}} =
               SegmentLedger.record(b3, bill.bill_id, fingerprint, 3, :rejected)

      {refund, quota} = thread_all_rejected(ledger, bill, fingerprint)
      assert refund == bill.rate_minor
      assert quota == bill.quota_debit
      assert refund == 9
      assert quota == 3
    end

    test "rejects invalid index, unsupported outcome, and foreign or malformed binding" do
      {bill, fingerprint, ledger} = open_ledger()
      {other, other_fp, _} = open_ledger(bill_id: "bill-other", rate_minor: 50)

      assert SegmentLedger.record(ledger, bill.bill_id, fingerprint, 0, :rejected) ==
               {:error, :invalid_index}

      assert SegmentLedger.record(ledger, bill.bill_id, fingerprint, 2, :rejected) ==
               {:error, :invalid_index}

      assert SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1.0, :rejected) ==
               {:error, :invalid_index}

      assert SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :ok) ==
               {:error, :invalid_outcome}

      assert SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :non_ok) ==
               {:error, :invalid_outcome}

      assert SegmentLedger.record(ledger, "bill-other", fingerprint, 1, :rejected) ==
               {:error, :billing_conflict}

      assert SegmentLedger.record(ledger, bill.bill_id, other_fp, 1, :rejected) ==
               {:error, :billing_conflict}

      assert SegmentLedger.record(ledger, other.bill_id, other_fp, 1, :rejected) ==
               {:error, :billing_conflict}

      malformed = %Fingerprint{version: 1, digest: <<0>>}

      assert SegmentLedger.record(ledger, bill.bill_id, malformed, 1, :rejected) ==
               {:error, :billing_conflict}

      assert SegmentLedger.record(ledger, bill.bill_id, :not_a_fingerprint, 1, :rejected) ==
               {:error, :billing_conflict}

      before = ledger

      assert SegmentLedger.record(ledger, bill.bill_id, fingerprint, 2, :accepted) ==
               {:error, :invalid_index}

      assert ledger == before
    end

    test "zero-price rejection credits one quota with zero refund" do
      {bill, fingerprint, ledger} =
        open_ledger(rate_minor: 0, precharge_percent: 50, segment_count: 7)

      assert {:ok, rejected, %{refund_minor: 0, quota_credit: 1}} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 4, :rejected)

      assert {:ok, :duplicate, ^rejected, @zero_delta} =
               SegmentLedger.record(rejected, bill.bill_id, fingerprint, 4, :rejected)

      {refund, quota} = thread_all_rejected(ledger, bill, fingerprint)
      assert refund == 0
      assert quota == 7
    end

    test "N=1 rejection refunds the full bill and index 2 is invalid" do
      {bill, fingerprint, ledger} = open_ledger(rate_minor: @max_int64, precharge_percent: 100)

      assert {:ok, rejected, %{refund_minor: @max_int64, quota_credit: 1}} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :rejected)

      assert SegmentLedger.record(rejected, bill.bill_id, fingerprint, 2, :rejected) ==
               {:error, :invalid_index}
    end

    test "terminal?/1 is true only when every index is accepted or rejected" do
      {bill, fingerprint, ledger} = open_ledger(segment_count: 2)
      refute SegmentLedger.terminal?(ledger)

      assert {:ok, accepted, _} =
               SegmentLedger.record(ledger, bill.bill_id, fingerprint, 1, :accepted)

      refute SegmentLedger.terminal?(accepted)

      assert {:ok, pending, _} =
               SegmentLedger.record(accepted, bill.bill_id, fingerprint, 2, :uncertain)

      refute SegmentLedger.terminal?(pending)

      assert {:ok, done, _} =
               SegmentLedger.record(accepted, bill.bill_id, fingerprint, 2, :rejected)

      assert SegmentLedger.terminal?(done)
    end
  end

  defp open_ledger(overrides \\ []) do
    {:ok, bill} = Bill.new(valid_attrs(overrides))
    {:ok, fingerprint} = Fingerprint.compute(bill)
    {:ok, ledger} = SegmentLedger.open(bill)
    {bill, fingerprint, ledger}
  end

  defp thread_all_rejected(ledger, bill, fingerprint) do
    Enum.reduce(1..bill.quota_debit, {ledger, 0, 0}, fn index, {current, refund, quota} ->
      assert {:ok, next, %{refund_minor: delta_refund, quota_credit: delta_quota}} =
               SegmentLedger.record(current, bill.bill_id, fingerprint, index, :rejected)

      {next, refund + delta_refund, quota + delta_quota}
    end)
    |> then(fn {_ledger, refund, quota} -> {refund, quota} end)
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
