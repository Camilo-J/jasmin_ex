defmodule JasminEx.Routing.State do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Clock
  alias JasminEx.Billing.Reservation
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.RouteTable
  alias JasminEx.Routing.User

  defstruct groups: %{},
            users: %{},
            routes: %RouteTable{},
            revision: 0,
            reservations: %{},
            tombstones: %{}

  @type t :: %__MODULE__{
          groups: %{optional(String.t()) => Group.t()},
          users: %{optional(String.t()) => User.t()},
          routes: RouteTable.t(),
          revision: non_neg_integer(),
          reservations: %{optional(binary()) => Reservation.t()},
          tombstones: map()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec put_group(t(), Group.t()) :: {:ok, t()}
  def put_group(%__MODULE__{} = state, %Group{gid: gid} = group) do
    {:ok, %{state | groups: Map.put(state.groups, gid, group)}}
  end

  @spec put_user(t(), User.t()) :: {:ok, t()} | {:error, :unknown_group | :duplicate_username}
  def put_user(%__MODULE__{} = state, %User{uid: uid, gid: gid, username: username} = user) do
    cond do
      not Map.has_key?(state.groups, gid) ->
        {:error, :unknown_group}

      username_taken?(state.users, uid, username) ->
        {:error, :duplicate_username}

      true ->
        {:ok, %{state | users: Map.put(state.users, uid, user)}}
    end
  end

  @spec put_route(t(), Route.t()) :: {:ok, t()} | {:error, :invalid_order}
  def put_route(%__MODULE__{} = state, %Route{} = route) do
    with {:ok, table} <- RouteTable.put(state.routes, route) do
      {:ok, %{state | routes: table}}
    end
  end

  @spec delete_group(t(), term()) :: {:ok, t()} | {:error, :unknown_group}
  def delete_group(%__MODULE__{} = state, gid) when is_binary(gid) do
    drop_group(state, Map.pop(state.groups, gid))
  end

  def delete_group(%__MODULE__{}, _gid), do: {:error, :unknown_group}

  @spec admit(t(), term(), Clock.clock()) :: {:ok, t()} | {:error, atom()}
  def admit(%__MODULE__{} = state, %Admission{bill: %Bill{} = bill} = admission, clock) do
    with {:ok, user} <- fetch_user(state, bill.uid),
         {:ok, _route} <- fetch_route(state, bill.route_order),
         {:ok, balance_minor} <- debit_balance(user.balance_minor, bill.rate_minor),
         {:ok, submit_quota} <- debit_quota(user.submit_quota, bill.quota_debit),
         {:ok, reservation} <- Reservation.open(admission, clock) do
      user = %{user | balance_minor: balance_minor, submit_quota: submit_quota}

      {:ok,
       %{
         state
         | users: Map.put(state.users, user.uid, user),
           reservations: Map.put(state.reservations, bill.bill_id, reservation)
       }}
    end
  end

  def admit(%__MODULE__{}, _admission, _clock), do: {:error, :invalid_bill_id}

  defp drop_group(_state, {nil, _groups}), do: {:error, :unknown_group}

  defp drop_group(state, {%Group{gid: gid}, groups}) do
    users = Map.reject(state.users, fn {_uid, user} -> user.gid == gid end)
    {:ok, %{state | groups: groups, users: users}}
  end

  defp username_taken?(users, uid, username) do
    Enum.any?(users, fn {existing_uid, user} ->
      existing_uid != uid and user.username == username
    end)
  end

  defp fetch_user(state, uid) do
    case Map.fetch(state.users, uid) do
      {:ok, user} -> {:ok, user}
      :error -> {:error, :unknown_user}
    end
  end

  defp fetch_route(state, order) do
    case Map.fetch(state.routes.routes, order) do
      {:ok, route} -> {:ok, route}
      :error -> {:error, :unknown_route}
    end
  end

  defp debit_balance(nil, _rate), do: {:ok, nil}

  defp debit_balance(balance, rate)
       when is_integer(balance) and is_integer(rate) and balance >= rate,
       do: {:ok, balance - rate}

  defp debit_balance(_balance, _rate), do: {:error, :insufficient_balance}

  defp debit_quota(nil, _debit), do: {:ok, nil}

  defp debit_quota(quota, debit) when is_integer(quota) and is_integer(debit) and quota >= debit,
    do: {:ok, quota - debit}

  defp debit_quota(_quota, _debit), do: {:error, :insufficient_quota}
end
