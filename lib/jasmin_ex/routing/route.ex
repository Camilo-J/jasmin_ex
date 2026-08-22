defmodule JasminEx.Routing.Route do
  @moduledoc false

  alias JasminEx.Routing.ConnectorRef

  @enforce_keys [:kind, :order, :connector, :filters]
  defstruct @enforce_keys

  @type kind :: :static | :default
  @type t :: %__MODULE__{
          kind: kind(),
          order: non_neg_integer(),
          connector: ConnectorRef.t(),
          filters: [term()]
        }

  @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_route}
  def new(attrs) when is_list(attrs) do
    kind = Keyword.get(attrs, :kind)
    order = Keyword.get(attrs, :order)
    connector = Keyword.get(attrs, :connector)
    filters = Keyword.get(attrs, :filters, [])

    if kind in [:static, :default] and is_integer(order) and order >= 0 and
         match?(%ConnectorRef{}, connector) and is_list(filters) do
      {:ok, %__MODULE__{kind: kind, order: order, connector: connector, filters: filters}}
    else
      {:error, :invalid_route}
    end
  end

  def new(_attrs), do: {:error, :invalid_route}
end
