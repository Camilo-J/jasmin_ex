defmodule JasminEx.Billing.SegmentLedger do
  @moduledoc false

  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint

  @max_int64 9_223_372_036_854_775_807
  @max_id_bytes 128
  @max_segment_count 255
  @outcomes [:accepted, :rejected, :uncertain]
  @zero_delta %{refund_minor: 0, quota_credit: 0}

  @enforce_keys [:bill_id, :fingerprint, :unit_price, :count, :outcomes]
  defstruct @enforce_keys

  @type outcome :: :accepted | :rejected | :uncertain
  @type delta :: %{refund_minor: non_neg_integer(), quota_credit: 0 | 1}

  @type t :: %__MODULE__{
          bill_id: binary(),
          fingerprint: Fingerprint.t(),
          unit_price: non_neg_integer(),
          count: 1..255,
          outcomes: %{optional(pos_integer()) => outcome()}
        }

  @spec open(term()) :: {:ok, t()} | {:error, :invalid_bill | :indivisible_bill}
  def open(%Bill{} = bill) do
    with :ok <- validate_bill(bill),
         {:ok, fingerprint} <- Fingerprint.compute(bill) do
      count = bill.quota_debit

      {:ok,
       %__MODULE__{
         bill_id: bill.bill_id,
         fingerprint: fingerprint,
         unit_price: div(bill.rate_minor, count),
         count: count,
         outcomes: %{}
       }}
    end
  end

  def open(_bill), do: {:error, :invalid_bill}

  @spec record(term(), term(), term(), term(), term()) ::
          {:ok, t(), delta()}
          | {:ok, :duplicate, t(), delta()}
          | {:error,
             :billing_conflict
             | :invalid_index
             | :invalid_outcome
             | :conflicting_settlement
             | :invalid_bill}
  def record(%__MODULE__{} = ledger, bill_id, fingerprint, index, outcome) do
    with :ok <- bind(ledger, bill_id, fingerprint),
         :ok <- validate_index(ledger, index),
         :ok <- validate_outcome(outcome) do
      apply_outcome(ledger, index, outcome)
    end
  end

  def record(_ledger, _bill_id, _fingerprint, _index, _outcome), do: {:error, :invalid_bill}

  defp validate_bill(%Bill{} = bill) do
    with :ok <- validate_id(bill.bill_id),
         :ok <- validate_id(bill.uid),
         :ok <- validate_route(bill.route_order),
         :ok <- validate_amount(bill.rate_minor),
         :ok <- validate_amount(bill.precharge_minor),
         :ok <- validate_amount(bill.remainder_minor),
         :ok <- validate_percent(bill.precharge_percent),
         :ok <- validate_count(bill.quota_debit),
         :ok <- validate_sum(bill),
         :ok <- validate_divisible(bill) do
      validate_canonical(bill)
    end
  end

  defp validate_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_id_bytes,
       do: :ok

  defp validate_id(_id), do: {:error, :invalid_bill}

  defp validate_route(order) when is_integer(order) and order >= 0 and order <= @max_int64,
    do: :ok

  defp validate_route(_order), do: {:error, :invalid_bill}

  defp validate_amount(amount) when is_integer(amount) and amount >= 0 and amount <= @max_int64,
    do: :ok

  defp validate_amount(_amount), do: {:error, :invalid_bill}

  defp validate_percent(percent) when is_integer(percent) and percent >= 0 and percent <= 100,
    do: :ok

  defp validate_percent(_percent), do: {:error, :invalid_bill}

  defp validate_count(count)
       when is_integer(count) and count >= 1 and count <= @max_segment_count,
       do: :ok

  defp validate_count(_count), do: {:error, :invalid_bill}

  defp validate_sum(%Bill{
         rate_minor: rate_minor,
         precharge_minor: precharge_minor,
         remainder_minor: remainder_minor
       })
       when precharge_minor + remainder_minor == rate_minor,
       do: :ok

  defp validate_sum(_bill), do: {:error, :invalid_bill}

  defp validate_divisible(%Bill{
         rate_minor: rate_minor,
         precharge_minor: precharge_minor,
         remainder_minor: remainder_minor,
         quota_debit: count
       })
       when rem(rate_minor, count) == 0 and rem(precharge_minor, count) == 0 and
              rem(remainder_minor, count) == 0,
       do: :ok

  defp validate_divisible(_bill), do: {:error, :indivisible_bill}

  defp validate_canonical(%Bill{} = bill) do
    attrs = [
      bill_id: bill.bill_id,
      uid: bill.uid,
      route_order: bill.route_order,
      rate_minor: div(bill.rate_minor, bill.quota_debit),
      precharge_percent: bill.precharge_percent,
      segment_count: bill.quota_debit
    ]

    case Bill.new(attrs) do
      {:ok, ^bill} -> :ok
      _other -> {:error, :invalid_bill}
    end
  end

  defp bind(%__MODULE__{bill_id: bill_id, fingerprint: fingerprint}, bill_id, fingerprint),
    do: :ok

  defp bind(_ledger, _bill_id, _fingerprint), do: {:error, :billing_conflict}

  defp validate_index(%__MODULE__{count: count}, index)
       when is_integer(index) and index >= 1 and index <= count,
       do: :ok

  defp validate_index(_ledger, _index), do: {:error, :invalid_index}

  defp validate_outcome(outcome) when outcome in @outcomes, do: :ok
  defp validate_outcome(_outcome), do: {:error, :invalid_outcome}

  defp apply_outcome(%__MODULE__{} = ledger, index, outcome) do
    case Map.get(ledger.outcomes, index) do
      ^outcome ->
        {:ok, :duplicate, ledger, @zero_delta}

      nil ->
        settle_new(ledger, index, outcome)

      :uncertain when outcome in [:accepted, :rejected] ->
        settle_new(ledger, index, outcome)

      _other ->
        {:error, :conflicting_settlement}
    end
  end

  defp settle_new(%__MODULE__{} = ledger, index, outcome) do
    ledger = %{ledger | outcomes: Map.put(ledger.outcomes, index, outcome)}
    {:ok, ledger, delta_for(outcome, ledger.unit_price)}
  end

  defp delta_for(:rejected, unit_price), do: %{refund_minor: unit_price, quota_credit: 1}
  defp delta_for(_outcome, _unit_price), do: @zero_delta
end
