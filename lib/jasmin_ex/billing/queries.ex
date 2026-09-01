defmodule JasminEx.Billing.Queries do
  @moduledoc false

  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.RouteTable
  alias JasminEx.Routing.State

  @spec quote(State.t(), Routable.t()) :: {:ok, non_neg_integer()} | {:error, :no_route}
  def quote(%State{routes: table}, %Routable{} = routable) do
    case RouteTable.winning_route(table, routable) do
      %{rate_minor: rate_minor} -> {:ok, rate_minor}
      nil -> {:error, :no_route}
    end
  end

  @spec balance(State.t(), term()) ::
          {:ok, non_neg_integer() | :unlimited} | {:error, :unknown_user}
  def balance(%State{users: users}, uid) when is_binary(uid) do
    case Map.fetch(users, uid) do
      {:ok, %{balance_minor: nil}} -> {:ok, :unlimited}
      {:ok, %{balance_minor: amount}} -> {:ok, amount}
      :error -> {:error, :unknown_user}
    end
  end

  def balance(%State{}, _uid), do: {:error, :unknown_user}
end
