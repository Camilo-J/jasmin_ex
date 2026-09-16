defmodule JasminEx.Routing.State do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Clock
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.Settlement
  alias JasminEx.Billing.Tombstone
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
          tombstones: %{optional(binary()) => Tombstone.t()}
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

  @max_int64 9_223_372_036_854_775_807

  @spec set_balance(t(), term(), term()) ::
          {:ok, t(), User.t()} | {:unchanged, User.t()} | {:error, atom()}
  def set_balance(%__MODULE__{} = state, uid, amount) when is_binary(uid) do
    put_user_amount(state, uid, amount, :balance_minor)
  end

  def set_balance(%__MODULE__{}, _uid, _amount), do: {:error, :unknown_user}

  @spec set_quota(t(), term(), term()) ::
          {:ok, t(), User.t()} | {:unchanged, User.t()} | {:error, atom()}
  def set_quota(%__MODULE__{} = state, uid, amount) when is_binary(uid) do
    put_user_amount(state, uid, amount, :submit_quota)
  end

  def set_quota(%__MODULE__{}, _uid, _amount), do: {:error, :unknown_user}

  @spec set_smpp_secret(t(), term(), term()) :: {:ok, t(), User.t()} | {:error, atom()}
  def set_smpp_secret(%__MODULE__{} = state, uid, secret) when is_binary(uid) do
    with {:ok, user} <- fetch_user(state, uid),
         {:ok, user} <- User.set_smpp_secret(user, secret) do
      {:ok, %{state | users: Map.put(state.users, uid, user)}, user}
    end
  end

  def set_smpp_secret(%__MODULE__{}, _uid, _secret), do: {:error, :unknown_user}

  @spec set_max_bindings(t(), term(), term()) ::
          {:ok, t(), User.t()} | {:unchanged, User.t()} | {:error, atom()}
  def set_max_bindings(%__MODULE__{} = state, uid, limit) when is_binary(uid) do
    with {:ok, user} <- fetch_user(state, uid),
         {:ok, user} <- User.set_max_bindings(user, limit) do
      if state.users[uid].max_bindings == user.max_bindings do
        {:unchanged, user}
      else
        {:ok, %{state | users: Map.put(state.users, uid, user)}, user}
      end
    end
  end

  def set_max_bindings(%__MODULE__{}, _uid, _limit), do: {:error, :unknown_user}

  @spec set_dlr_level(t(), term(), term()) ::
          {:ok, t(), User.t()} | {:unchanged, User.t()} | {:error, atom()}
  def set_dlr_level(%__MODULE__{} = state, uid, value) when is_binary(uid) do
    put_user_dlr_permission(state, uid, value, :set_dlr_level)
  end

  def set_dlr_level(%__MODULE__{}, _uid, _value), do: {:error, :unknown_user}

  @spec set_http_set_dlr_method(t(), term(), term()) ::
          {:ok, t(), User.t()} | {:unchanged, User.t()} | {:error, atom()}
  def set_http_set_dlr_method(%__MODULE__{} = state, uid, value) when is_binary(uid) do
    put_user_dlr_permission(state, uid, value, :http_set_dlr_method)
  end

  def set_http_set_dlr_method(%__MODULE__{}, _uid, _value), do: {:error, :unknown_user}

  @spec set_rate(t(), term(), term()) ::
          {:ok, t(), Route.t()} | {:unchanged, Route.t()} | {:error, atom()}
  def set_rate(%__MODULE__{} = state, order, rate) when is_integer(order) and order >= 0 do
    with {:ok, rate} <- validate_rate(rate),
         {:ok, route} <- fetch_route(state, order) do
      if route.rate_minor == rate do
        {:unchanged, route}
      else
        route = %{route | rate_minor: rate}
        {:ok, table} = RouteTable.put(state.routes, route)
        {:ok, %{state | routes: table}, route}
      end
    end
  end

  def set_rate(%__MODULE__{}, _order, _rate), do: {:error, :unknown_route}

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

  @spec settle(t(), term()) ::
          {:ok, t()} | {:ok, :duplicate} | {:ok, :late_ignored} | {:error, atom()}
  def settle(%__MODULE__{} = state, %Settlement{bill_id: bill_id} = settlement) do
    case {Map.get(state.tombstones, bill_id), Map.get(state.reservations, bill_id)} do
      {%Tombstone{} = stone, _} -> Tombstone.classify(stone, settlement)
      {_, %Reservation{} = reservation} -> open_settle(state, reservation, settlement)
      {nil, nil} -> {:error, :unknown_bill}
    end
  end

  def settle(%__MODULE__{}, _settlement), do: {:error, :invalid_bill_id}

  @spec expire_due(t(), Clock.clock()) :: {:ok, t(), non_neg_integer()}
  def expire_due(%__MODULE__{} = state, clock) do
    now = Clock.monotonic_ms(clock)

    due =
      Enum.filter(state.reservations, fn {_id, reservation} ->
        reservation.monotonic_deadline_ms <= now
      end)

    {:ok, Enum.reduce(due, state, &expire_one/2), length(due)}
  end

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

  defp put_user_dlr_permission(state, uid, value, field) do
    with {:ok, user} <- fetch_user(state, uid),
         {:ok, user} <- apply_dlr_permission(user, field, value) do
      if Map.fetch!(state.users[uid], field) == Map.fetch!(user, field) do
        {:unchanged, user}
      else
        {:ok, %{state | users: Map.put(state.users, uid, user)}, user}
      end
    end
  end

  defp apply_dlr_permission(user, :set_dlr_level, value), do: User.set_dlr_level(user, value)

  defp apply_dlr_permission(user, :http_set_dlr_method, value),
    do: User.set_http_set_dlr_method(user, value)

  defp put_user_amount(state, uid, amount, field) do
    with {:ok, amount} <- validate_optional_amount(amount),
         {:ok, user} <- fetch_user(state, uid) do
      if Map.fetch!(user, field) == amount do
        {:unchanged, user}
      else
        user = Map.put(user, field, amount)
        {:ok, %{state | users: Map.put(state.users, uid, user)}, user}
      end
    end
  end

  defp validate_optional_amount(nil), do: {:ok, nil}

  defp validate_optional_amount(amount) when is_integer(amount) and amount < 0,
    do: {:error, :invalid_amount}

  defp validate_optional_amount(amount) when is_integer(amount) and amount > @max_int64,
    do: {:error, :amount_overflow}

  defp validate_optional_amount(amount) when is_integer(amount), do: {:ok, amount}
  defp validate_optional_amount(_amount), do: {:error, :invalid_amount}

  defp validate_rate(rate) when is_integer(rate) and rate >= 0 and rate <= @max_int64,
    do: {:ok, rate}

  defp validate_rate(rate) when is_integer(rate) and rate > @max_int64,
    do: {:error, :amount_overflow}

  defp validate_rate(_rate), do: {:error, :invalid_amount}

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

  defp open_settle(state, reservation, %Settlement{fingerprint: fingerprint, outcome: outcome}) do
    cond do
      reservation.fingerprint != fingerprint ->
        {:error, :billing_conflict}

      outcome == :ok ->
        close(state, reservation, :settled_ok, 0)

      outcome == :non_ok ->
        close(state, reservation, :settled_non_ok, reservation.refundable_minor)

      true ->
        {:error, :invalid_bill_id}
    end
  end

  defp expire_one({_bill_id, reservation}, state) do
    {:ok, next} = close(state, reservation, :expired, reservation.refundable_minor)
    next
  end

  defp close(state, reservation, tombstone_state, credit) do
    with {:ok, user} <- fetch_user(state, reservation.uid),
         {:ok, balance_minor} <- credit_balance(user.balance_minor, credit),
         {:ok, stone} <- Tombstone.seal(reservation, tombstone_state) do
      user = %{user | balance_minor: balance_minor}

      {:ok,
       %{
         state
         | users: Map.put(state.users, user.uid, user),
           reservations: Map.delete(state.reservations, reservation.bill_id),
           tombstones: Map.put(state.tombstones, reservation.bill_id, stone)
       }}
    end
  end

  defp credit_balance(nil, _amount), do: {:ok, nil}

  defp credit_balance(balance, amount)
       when is_integer(balance) and is_integer(amount) and amount >= 0,
       do: {:ok, balance + amount}
end
