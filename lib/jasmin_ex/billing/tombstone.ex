defmodule JasminEx.Billing.Tombstone do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.Settlement

  @states [:settled_ok, :settled_non_ok, :expired]
  @enforce_keys [:bill_id, :fingerprint, :state]
  defstruct @enforce_keys

  @type state :: :settled_ok | :settled_non_ok | :expired
  @type t :: %__MODULE__{bill_id: binary(), fingerprint: Fingerprint.t(), state: state()}

  @spec seal(Reservation.t(), state()) :: {:ok, t()} | {:error, :invalid_bill_id}
  def seal(%Reservation{bill_id: bill_id, fingerprint: fingerprint}, state)
      when state in @states do
    {:ok, %__MODULE__{bill_id: bill_id, fingerprint: fingerprint, state: state}}
  end

  def seal(_reservation, _state), do: {:error, :invalid_bill_id}

  @spec identity(t()) :: {binary(), Fingerprint.t(), :ok | :non_ok | :expired}
  def identity(%__MODULE__{bill_id: bill_id, fingerprint: fingerprint, state: state}) do
    {bill_id, fingerprint, semantics(state)}
  end

  @spec classify(t(), Admission.t() | Settlement.t()) ::
          {:ok, :duplicate, Bill.t(), Fingerprint.t()}
          | {:ok, :duplicate | :late_ignored}
          | {:error, :billing_conflict | :conflicting_settlement}
  def classify(%__MODULE__{} = tombstone, %Admission{} = admission) do
    {tombstone_id, tombstone_fp, _semantics} = identity(tombstone)
    {admission_id, admission_fp, :admit} = Admission.identity(admission)

    if tombstone_id == admission_id and tombstone_fp == admission_fp do
      {:ok, :duplicate, admission.bill, admission_fp}
    else
      {:error, :billing_conflict}
    end
  end

  def classify(%__MODULE__{} = tombstone, %Settlement{} = settlement) do
    {tombstone_id, tombstone_fp, semantics} = identity(tombstone)
    {settlement_id, settlement_fp, outcome} = Settlement.identity(settlement)

    cond do
      tombstone_id != settlement_id or tombstone_fp != settlement_fp ->
        {:error, :billing_conflict}

      semantics == :expired ->
        {:ok, :late_ignored}

      semantics == outcome ->
        {:ok, :duplicate}

      true ->
        {:error, :conflicting_settlement}
    end
  end

  defp semantics(:settled_ok), do: :ok
  defp semantics(:settled_non_ok), do: :non_ok
  defp semantics(:expired), do: :expired
end
