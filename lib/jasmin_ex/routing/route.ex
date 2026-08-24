defmodule JasminEx.Routing.Route do
  @moduledoc false

  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter

  @enforce_keys [:kind, :order, :connector, :filters]
  defstruct [:kind, :order, :connector, :filters, rate_minor: 0, precharge_percent: 0]

  @type kind :: :static | :default
  @type t :: %__MODULE__{
          kind: kind(),
          order: non_neg_integer(),
          connector: ConnectorRef.t(),
          filters: [Filter.t()],
          rate_minor: non_neg_integer(),
          precharge_percent: 0..100
        }

  @max_int64 9_223_372_036_854_775_807

  @spec new(keyword()) ::
          {:ok, t()}
          | {:error,
             :invalid_route
             | :invalid_connector
             | :invalid_filter
             | :invalid_amount
             | :amount_overflow
             | :invalid_percentage}
  def new(attrs) when is_list(attrs) do
    kind = Keyword.get(attrs, :kind)
    order = Keyword.get(attrs, :order)
    connector = Keyword.get(attrs, :connector)
    filters = Keyword.get(attrs, :filters, [])
    rate_minor = Keyword.get(attrs, :rate_minor, 0)
    precharge_percent = Keyword.get(attrs, :precharge_percent, 0)

    cond do
      not (kind in [:static, :default] and is_integer(order) and order >= 0 and is_list(filters)) ->
        {:error, :invalid_route}

      not valid_connector?(connector) ->
        {:error, :invalid_connector}

      not Enum.all?(filters, &Filter.valid?/1) ->
        {:error, :invalid_filter}

      true ->
        build(kind, order, connector, filters, rate_minor, precharge_percent)
    end
  end

  def new(_attrs), do: {:error, :invalid_route}

  defp valid_connector?(%ConnectorRef{type: :smpp_client, id: id}),
    do: match?({:ok, _}, ConnectorRef.new(id))

  defp valid_connector?(_connector), do: false

  defp build(kind, order, connector, filters, rate_minor, precharge_percent) do
    with {:ok, rate_minor} <- validate_rate(rate_minor),
         {:ok, precharge_percent} <- validate_percent(precharge_percent) do
      {:ok,
       %__MODULE__{
         kind: kind,
         order: order,
         connector: connector,
         filters: filters,
         rate_minor: rate_minor,
         precharge_percent: precharge_percent
       }}
    end
  end

  defp validate_rate(rate) when is_integer(rate) and rate >= 0 and rate <= @max_int64,
    do: {:ok, rate}

  defp validate_rate(rate) when is_integer(rate) and rate > @max_int64,
    do: {:error, :amount_overflow}

  defp validate_rate(_rate), do: {:error, :invalid_amount}

  defp validate_percent(percent) when is_integer(percent) and percent >= 0 and percent <= 100,
    do: {:ok, percent}

  defp validate_percent(_percent), do: {:error, :invalid_percentage}
end
