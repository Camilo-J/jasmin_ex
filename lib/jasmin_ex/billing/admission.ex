defmodule JasminEx.Billing.Admission do
  @moduledoc false

  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint

  @max_int64 9_223_372_036_854_775_807
  @enforce_keys [:bill, :ttl_ms]
  defstruct @enforce_keys

  @type t :: %__MODULE__{bill: Bill.t(), ttl_ms: non_neg_integer()}

  @spec new(term()) :: {:ok, t()} | {:error, :invalid_bill_id | :invalid_ttl}
  def new(attrs) when is_list(attrs) do
    with {:ok, bill} <- require_bill(Keyword.get(attrs, :bill)),
         {:ok, ttl_ms} <- validate_ttl(Keyword.get(attrs, :ttl_ms)) do
      {:ok, %__MODULE__{bill: bill, ttl_ms: ttl_ms}}
    end
  end

  def new(_attrs), do: {:error, :invalid_bill_id}

  @spec identity(t()) :: {binary(), Fingerprint.t(), :admit}
  def identity(%__MODULE__{bill: %Bill{bill_id: bill_id} = bill}) do
    {:ok, fingerprint} = Fingerprint.compute(bill)
    {bill_id, fingerprint, :admit}
  end

  defp require_bill(%Bill{} = bill), do: {:ok, bill}
  defp require_bill(_bill), do: {:error, :invalid_bill_id}

  defp validate_ttl(ttl) when is_integer(ttl) and ttl >= 0 and ttl <= @max_int64, do: {:ok, ttl}
  defp validate_ttl(_ttl), do: {:error, :invalid_ttl}
end
