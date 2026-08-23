defmodule JasminEx.Billing.Bill do
  @moduledoc false

  @max_int64 9_223_372_036_854_775_807
  @max_id_bytes 128
  @quota_debit 1

  @enforce_keys [
    :bill_id,
    :uid,
    :route_order,
    :rate_minor,
    :precharge_percent,
    :precharge_minor,
    :remainder_minor,
    :quota_debit
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          bill_id: binary(),
          uid: binary(),
          route_order: non_neg_integer(),
          rate_minor: non_neg_integer(),
          precharge_percent: 0..100,
          precharge_minor: non_neg_integer(),
          remainder_minor: non_neg_integer(),
          quota_debit: 1
        }

  @spec new(term()) ::
          {:ok, t()}
          | {:error,
             :invalid_bill_id
             | :invalid_uid
             | :invalid_route
             | :invalid_amount
             | :amount_overflow
             | :invalid_percentage}
  def new(attrs) when is_list(attrs) do
    with {:ok, bill_id} <- validate_id(Keyword.get(attrs, :bill_id), :invalid_bill_id),
         {:ok, uid} <- validate_id(Keyword.get(attrs, :uid), :invalid_uid),
         {:ok, route_order} <- validate_route_order(Keyword.get(attrs, :route_order)),
         {:ok, rate_minor} <- validate_rate(Keyword.get(attrs, :rate_minor)),
         {:ok, percent} <- validate_percent(Keyword.get(attrs, :precharge_percent)) do
      {precharge_minor, remainder_minor} = split(rate_minor, percent)

      {:ok,
       %__MODULE__{
         bill_id: bill_id,
         uid: uid,
         route_order: route_order,
         rate_minor: rate_minor,
         precharge_percent: percent,
         precharge_minor: precharge_minor,
         remainder_minor: remainder_minor,
         quota_debit: @quota_debit
       }}
    end
  end

  def new(_attrs), do: {:error, :invalid_bill_id}

  defp validate_id(id, _error)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_id_bytes,
       do: {:ok, id}

  defp validate_id(_id, error), do: {:error, error}

  defp validate_route_order(order)
       when is_integer(order) and order >= 0 and order <= @max_int64,
       do: {:ok, order}

  defp validate_route_order(_order), do: {:error, :invalid_route}

  defp validate_rate(rate) when is_integer(rate) and rate < 0, do: {:error, :invalid_amount}

  defp validate_rate(rate) when is_integer(rate) and rate > @max_int64,
    do: {:error, :amount_overflow}

  defp validate_rate(rate) when is_integer(rate), do: {:ok, rate}
  defp validate_rate(_rate), do: {:error, :invalid_amount}

  defp validate_percent(percent) when is_integer(percent) and percent >= 0 and percent <= 100,
    do: {:ok, percent}

  defp validate_percent(_percent), do: {:error, :invalid_percentage}

  defp split(rate_minor, percent) do
    precharge_minor = div(rate_minor * percent, 100)
    {precharge_minor, rate_minor - precharge_minor}
  end
end

defimpl Inspect, for: JasminEx.Billing.Bill do
  def inspect(%JasminEx.Billing.Bill{bill_id: bill_id, uid: uid, route_order: route_order}, _opts) do
    "#JasminEx.Billing.Bill<bill_id: #{inspect(bill_id)}, uid: #{inspect(uid)}, route_order: #{route_order}, REDACTED>"
  end
end
