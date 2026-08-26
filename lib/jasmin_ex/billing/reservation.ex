defmodule JasminEx.Billing.Reservation do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Clock
  alias JasminEx.Billing.Fingerprint

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @enforce_keys [
    :bill_id,
    :uid,
    :fingerprint,
    :state,
    :captured_minor,
    :reserved_minor,
    :refundable_minor,
    :wall_deadline_ms,
    :monotonic_deadline_ms
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec open(Admission.t(), Clock.clock()) :: {:ok, t()} | {:error, :invalid_ttl}
  def open(%Admission{bill: %Bill{} = bill, ttl_ms: ttl_ms}, clock) do
    {:ok, fingerprint} = Fingerprint.compute(bill)

    with {:ok, wall_deadline_ms} <- add_deadline(Clock.wall_ms(clock), ttl_ms),
         {:ok, monotonic_deadline_ms} <- add_deadline(Clock.monotonic_ms(clock), ttl_ms) do
      {:ok,
       %__MODULE__{
         bill_id: bill.bill_id,
         uid: bill.uid,
         fingerprint: fingerprint,
         state: :open,
         captured_minor: bill.precharge_minor,
         reserved_minor: bill.remainder_minor,
         refundable_minor: bill.remainder_minor,
         wall_deadline_ms: wall_deadline_ms,
         monotonic_deadline_ms: monotonic_deadline_ms
       }}
    end
  end

  @spec identity(t()) :: {binary(), Fingerprint.t(), :admit}
  def identity(%__MODULE__{bill_id: bill_id, fingerprint: fingerprint}) do
    {bill_id, fingerprint, :admit}
  end

  @spec classify(t(), Admission.t()) ::
          {:ok, :duplicate, Bill.t(), Fingerprint.t()} | {:error, :billing_conflict}
  def classify(%__MODULE__{} = reservation, %Admission{} = admission) do
    {reservation_id, reservation_fp, :admit} = identity(reservation)
    {admission_id, admission_fp, :admit} = Admission.identity(admission)

    if reservation_id == admission_id and reservation_fp == admission_fp do
      {:ok, :duplicate, admission.bill, admission_fp}
    else
      {:error, :billing_conflict}
    end
  end

  defp add_deadline(base, ttl) when is_integer(base) and is_integer(ttl) do
    sum = base + ttl
    if sum >= @min_int64 and sum <= @max_int64, do: {:ok, sum}, else: {:error, :invalid_ttl}
  end
end
