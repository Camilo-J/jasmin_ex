defmodule JasminEx.Routing.RouteTable do
  @moduledoc false

  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.Route

  defstruct routes: %{}, descending_keys: []

  @type t :: %__MODULE__{
          routes: %{optional(non_neg_integer()) => Route.t()},
          descending_keys: [non_neg_integer()]
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec put(t(), Route.t()) :: {:ok, t()} | {:error, :invalid_order}
  def put(%__MODULE__{}, %Route{kind: :static, order: 0}), do: {:error, :invalid_order}

  def put(%__MODULE__{} = table, %Route{order: order} = route)
      when is_integer(order) and order >= 0 do
    routes = Map.put(table.routes, order, route)
    {:ok, %{table | routes: routes, descending_keys: Enum.sort(Map.keys(routes), :desc)}}
  end

  @spec resolve(t(), Routable.t()) :: {:ok, ConnectorRef.t()} | {:error, :no_route}
  def resolve(%__MODULE__{} = table, %Routable{} = routable) do
    case first_static_match(table, routable) || default_route(table) do
      %Route{connector: connector} -> {:ok, connector}
      nil -> {:error, :no_route}
    end
  end

  defp first_static_match(table, routable) do
    Enum.find_value(table.descending_keys, fn order ->
      static_match(Map.fetch!(table.routes, order), routable)
    end)
  end

  defp static_match(%Route{kind: :static, filters: filters} = route, routable) do
    if Filter.match?(filters, routable), do: route
  end

  defp static_match(_route, _routable), do: nil

  defp default_route(%{routes: routes}) do
    case Map.get(routes, 0) do
      %Route{kind: :default} = route -> route
      _missing -> nil
    end
  end
end
