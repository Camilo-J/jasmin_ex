defmodule JasminEx.Routing.Route do
  @moduledoc false

  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter

  @enforce_keys [:kind, :order, :connector, :filters]
  defstruct @enforce_keys

  @type kind :: :static | :default
  @type t :: %__MODULE__{
          kind: kind(),
          order: non_neg_integer(),
          connector: ConnectorRef.t(),
          filters: [Filter.t()]
        }

  @spec new(keyword()) ::
          {:ok, t()} | {:error, :invalid_route | :invalid_connector | :invalid_filter}
  def new(attrs) when is_list(attrs) do
    kind = Keyword.get(attrs, :kind)
    order = Keyword.get(attrs, :order)
    connector = Keyword.get(attrs, :connector)
    filters = Keyword.get(attrs, :filters, [])

    cond do
      not (kind in [:static, :default] and is_integer(order) and order >= 0 and is_list(filters)) ->
        {:error, :invalid_route}

      not valid_connector?(connector) ->
        {:error, :invalid_connector}

      not Enum.all?(filters, &Filter.valid?/1) ->
        {:error, :invalid_filter}

      true ->
        {:ok, %__MODULE__{kind: kind, order: order, connector: connector, filters: filters}}
    end
  end

  def new(_attrs), do: {:error, :invalid_route}

  defp valid_connector?(%ConnectorRef{type: :smpp_client, id: id}),
    do: match?({:ok, _}, ConnectorRef.new(id))

  defp valid_connector?(_connector), do: false
end
