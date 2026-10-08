defmodule JasminEx.Routing.State do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Clock
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.SegmentDispatch
  alias JasminEx.Billing.SegmentLedger
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
            tombstones: %{},
            segment_dispatches: %{}

  @type t :: %__MODULE__{
          groups: %{optional(String.t()) => Group.t()},
          users: %{optional(String.t()) => User.t()},
          routes: RouteTable.t(),
          revision: non_neg_integer(),
          reservations: %{optional(binary()) => Reservation.t()},
          tombstones: %{optional(binary()) => Tombstone.t()},
          segment_dispatches: %{optional(binary()) => SegmentDispatch.t()}
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
  def admit(%__MODULE__{} = state, %Admission{bill: %Bill{}} = admission, clock) do
    admit_with(state, admission, clock, &Reservation.open/2)
  end

  def admit(%__MODULE__{}, _admission, _clock), do: {:error, :invalid_bill_id}

  @spec admit_segments(t(), term(), Clock.clock()) :: {:ok, t()} | {:error, atom()}
  def admit_segments(%__MODULE__{} = state, %Admission{bill: %Bill{}} = admission, clock) do
    admit_with(state, admission, clock, &Reservation.open_segments/2)
  end

  def admit_segments(%__MODULE__{}, _admission, _clock), do: {:error, :invalid_bill_id}

  @spec admit_segments_with_dispatch(t(), term(), term(), Clock.clock()) ::
          {:ok, t()} | {:ok, :duplicate} | {:error, atom()}
  def admit_segments_with_dispatch(
        %__MODULE__{} = state,
        %Admission{bill: %Bill{}} = admission,
        children,
        clock
      ) do
    with {:ok, dispatch} <- SegmentDispatch.plan(admission.bill, children) do
      replay_or_admit(state, admission, dispatch, clock)
    end
  end

  def admit_segments_with_dispatch(%__MODULE__{}, _admission, _children, _clock),
    do: {:error, :invalid_bill_id}

  @spec claim_dispatch(t(), term(), term()) :: {:ok, t()} | {:error, atom()}
  def claim_dispatch(%__MODULE__{} = state, bill_id, gateway_id) when is_binary(bill_id) do
    update_dispatch(state, bill_id, &SegmentDispatch.claim(&1, gateway_id))
  end

  def claim_dispatch(%__MODULE__{}, _bill_id, _gateway_id), do: {:error, :invalid_bill_id}

  @spec record_dispatch(t(), term(), term(), term()) ::
          {:ok, t()} | {:ok, :duplicate} | {:error, atom()}
  def record_dispatch(%__MODULE__{} = state, bill_id, gateway_id, :queued)
      when is_binary(bill_id) do
    update_dispatch(state, bill_id, &SegmentDispatch.confirm_queued(&1, gateway_id))
  end

  def record_dispatch(%__MODULE__{} = state, bill_id, gateway_id, :rejected)
      when is_binary(bill_id) do
    with {:ok, next} <-
           update_dispatch(state, bill_id, &SegmentDispatch.record_failure(&1, gateway_id)) do
      recover_dispatch(next, bill_id)
    end
  end

  def record_dispatch(%__MODULE__{} = state, bill_id, gateway_id, :uncertain)
      when is_binary(bill_id) do
    with {:ok, dispatch} <- fetch_dispatch(state, bill_id),
         true <- claimed_child?(dispatch, gateway_id) do
      recover_dispatch(state, bill_id)
    else
      false -> {:error, :invalid_dispatch}
      error -> error
    end
  end

  def record_dispatch(%__MODULE__{}, _bill_id, _gateway_id, _outcome),
    do: {:error, :invalid_bill_id}

  @spec recover_dispatch(t(), term()) :: {:ok, t()} | {:ok, :duplicate} | {:error, atom()}
  def recover_dispatch(%__MODULE__{} = state, bill_id) when is_binary(bill_id) do
    case Map.get(state.segment_dispatches, bill_id) do
      nil -> {:error, :unknown_bill}
      %SegmentDispatch{phase: :closed} -> {:ok, :duplicate}
      dispatch -> compensate_dispatch(state, dispatch)
    end
  end

  def recover_dispatch(%__MODULE__{}, _bill_id), do: {:error, :invalid_bill_id}

  @spec recover_open_dispatches(t()) :: {:ok, t()} | {:ok, :unchanged} | {:error, atom()}
  def recover_open_dispatches(%__MODULE__{} = state) do
    Enum.reduce_while(state.segment_dispatches, {:ok, state, false}, fn {bill_id, dispatch},
                                                                        {:ok, acc, changed} ->
      recover_open_one(acc, bill_id, dispatch, changed)
    end)
    |> finish_open_recovery()
  end

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

  @spec settle_segment(t(), term(), term(), term(), term()) ::
          {:ok, t()} | {:ok, :duplicate | :late_ignored} | {:error, atom()}
  def settle_segment(%__MODULE__{} = state, bill_id, fingerprint, index, outcome)
      when is_binary(bill_id) do
    case {Map.get(state.tombstones, bill_id), Map.get(state.reservations, bill_id)} do
      {%Tombstone{} = stone, _} ->
        late_segment(stone, fingerprint)

      {_, %Reservation{ledger: nil}} ->
        {:error, :billing_conflict}

      {_, %Reservation{} = reservation} ->
        apply_segment(state, reservation, fingerprint, index, outcome)

      {nil, nil} ->
        {:error, :unknown_bill}
    end
  end

  def settle_segment(%__MODULE__{}, _bill_id, _fingerprint, _index, _outcome),
    do: {:error, :invalid_bill_id}

  @spec expire_due(t(), Clock.clock()) :: {:ok, t(), non_neg_integer()} | {:error, atom()}
  def expire_due(%__MODULE__{} = state, clock) do
    now = Clock.monotonic_ms(clock)

    due =
      Enum.filter(state.reservations, fn {_id, reservation} ->
        reservation.monotonic_deadline_ms <= now and not Reservation.segment_mode?(reservation)
      end)

    expire_batch(due, state)
  end

  defp drop_group(_state, {nil, _groups}), do: {:error, :unknown_group}

  defp drop_group(state, {%Group{gid: gid}, groups}) do
    users = Map.reject(state.users, fn {_uid, user} -> user.gid == gid end)
    {:ok, %{state | groups: groups, users: users}}
  end

  defp put_dispatch(state, %SegmentDispatch{bill_id: bill_id} = dispatch) do
    %{state | segment_dispatches: Map.put(state.segment_dispatches, bill_id, dispatch)}
  end

  defp update_dispatch(state, bill_id, fun) do
    with {:ok, dispatch} <- fetch_dispatch(state, bill_id),
         {:ok, next} <- fun.(dispatch) do
      {:ok, put_dispatch(state, next)}
    end
  end

  defp fetch_dispatch(state, bill_id) do
    case Map.fetch(state.segment_dispatches, bill_id) do
      {:ok, dispatch} -> {:ok, dispatch}
      :error -> {:error, :unknown_bill}
    end
  end

  defp claimed_child?(%SegmentDispatch{children: children}, gateway_id) do
    match?(
      %SegmentDispatch.Child{status: :claimed},
      Enum.find(children, &(&1.gateway_id == gateway_id))
    )
  end

  defp compensate_dispatch(state, dispatch) do
    with {:ok, next} <- apply_recovery_settlements(state, dispatch),
         {:ok, closed} <- SegmentDispatch.close(dispatch, close_outcome(dispatch)) do
      {:ok, put_dispatch(next, closed)}
    end
  end

  defp apply_recovery_settlements(state, dispatch) do
    dispatch
    |> SegmentDispatch.recovery_actions()
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, state}, fn {%{action: action}, index}, {:ok, acc} ->
      case settle_recovery_action(acc, dispatch, action, index) do
        {:ok, :duplicate} -> {:cont, {:ok, acc}}
        {:ok, :late_ignored} -> {:cont, {:ok, acc}}
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp settle_recovery_action(state, _dispatch, :hold_queued, _index), do: {:ok, state}

  defp settle_recovery_action(state, dispatch, :refund, index) do
    settle_segment(state, dispatch.bill_id, dispatch.fingerprint, index, :rejected)
  end

  defp settle_recovery_action(state, dispatch, :reject, index) do
    settle_segment(state, dispatch.bill_id, dispatch.fingerprint, index, :rejected)
  end

  defp settle_recovery_action(state, dispatch, :hold_uncertain, index) do
    preserve_or_hold_claimed(state, dispatch, index)
  end

  defp preserve_or_hold_claimed(state, dispatch, index) do
    case {Map.get(state.tombstones, dispatch.bill_id),
          Map.get(state.reservations, dispatch.bill_id)} do
      {%Tombstone{} = stone, _reservation} ->
        preserve_closed_identity(state, dispatch, stone)

      {_stone, %Reservation{} = reservation} ->
        preserve_open_claimed(state, dispatch, reservation, index)

      {nil, nil} ->
        {:error, :unknown_bill}
    end
  end

  defp preserve_closed_identity(state, dispatch, %Tombstone{fingerprint: fingerprint}) do
    if fingerprint == dispatch.fingerprint do
      {:ok, state}
    else
      {:error, :billing_conflict}
    end
  end

  defp preserve_open_claimed(state, dispatch, reservation, index) do
    with :ok <- match_dispatch_identity(dispatch, reservation),
         {:ok, outcome} <- claimed_ledger_outcome(reservation.ledger, index) do
      hold_claimed_outcome(state, dispatch, index, outcome)
    end
  end

  defp match_dispatch_identity(
         %SegmentDispatch{bill_id: bill_id, fingerprint: fingerprint},
         %Reservation{
           bill_id: bill_id,
           fingerprint: fingerprint,
           ledger: %SegmentLedger{bill_id: bill_id, fingerprint: fingerprint}
         }
       ),
       do: :ok

  defp match_dispatch_identity(_dispatch, _reservation), do: {:error, :billing_conflict}

  defp claimed_ledger_outcome(%SegmentLedger{count: count, outcomes: outcomes}, index)
       when is_integer(index) and index >= 1 and index <= count do
    {:ok, Map.get(outcomes, index)}
  end

  defp claimed_ledger_outcome(_ledger, _index), do: {:error, :invalid_index}

  defp hold_claimed_outcome(state, _dispatch, _index, outcome)
       when outcome in [:accepted, :rejected, :uncertain],
       do: {:ok, state}

  defp hold_claimed_outcome(state, dispatch, index, nil) do
    settle_segment(state, dispatch.bill_id, dispatch.fingerprint, index, :uncertain)
  end

  defp hold_claimed_outcome(_state, _dispatch, _index, _outcome),
    do: {:error, :conflicting_settlement}

  defp close_outcome(dispatch) do
    statuses = Enum.map(dispatch.children, & &1.status)

    cond do
      :claimed in statuses -> :uncertain
      :failed in statuses -> :rejected
      :queued in statuses -> :uncertain
      true -> :rejected
    end
  end

  defp recover_open_one(state, _bill_id, %SegmentDispatch{phase: :closed}, changed) do
    {:cont, {:ok, state, changed}}
  end

  defp recover_open_one(state, bill_id, _dispatch, changed) do
    case recover_dispatch(state, bill_id) do
      {:ok, :duplicate} -> {:cont, {:ok, state, changed}}
      {:ok, next} -> {:cont, {:ok, next, true}}
      error -> {:halt, error}
    end
  end

  defp finish_open_recovery({:ok, _state, false}), do: {:ok, :unchanged}
  defp finish_open_recovery({:ok, state, true}), do: {:ok, state}
  defp finish_open_recovery(error), do: error

  defp replay_or_admit(
         state,
         %Admission{bill: %Bill{bill_id: bill_id}} = admission,
         dispatch,
         clock
       ) do
    case {Map.get(state.tombstones, bill_id), Map.get(state.reservations, bill_id),
          Map.get(state.segment_dispatches, bill_id)} do
      {%Tombstone{}, _reservation, _existing} ->
        {:error, :billing_conflict}

      {_stone, %Reservation{} = reservation, %SegmentDispatch{} = existing} ->
        replay_dispatch(reservation, existing, admission, dispatch)

      {_stone, %Reservation{}, _existing} ->
        {:error, :billing_conflict}

      {_stone, _reservation, %SegmentDispatch{}} ->
        {:error, :billing_conflict}

      {nil, nil, nil} ->
        admit_new_dispatch(state, admission, dispatch, clock)
    end
  end

  defp replay_dispatch(reservation, existing, admission, dispatch) do
    case Reservation.classify(reservation, admission) do
      {:ok, :duplicate, _bill, _fingerprint} ->
        if SegmentDispatch.same_bound_plan?(existing, dispatch) do
          {:ok, :duplicate}
        else
          {:error, :billing_conflict}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp admit_new_dispatch(state, admission, dispatch, clock) do
    with {:ok, admitted} <- admit_segments(state, admission, clock),
         {:ok, reservation} <- fetch_reservation(admitted, dispatch.bill_id),
         {:ok, dispatch} <- SegmentDispatch.bind(dispatch, reservation) do
      {:ok, put_dispatch(admitted, dispatch)}
    end
  end

  defp fetch_reservation(state, bill_id) do
    case Map.fetch(state.reservations, bill_id) do
      {:ok, reservation} -> {:ok, reservation}
      :error -> {:error, :inconsistent_admission}
    end
  end

  defp admit_with(state, %Admission{bill: %Bill{} = bill} = admission, clock, opener) do
    with {:ok, user} <- fetch_user(state, bill.uid),
         {:ok, _route} <- fetch_route(state, bill.route_order),
         {:ok, balance_minor} <- debit_balance(user.balance_minor, bill.rate_minor),
         {:ok, submit_quota} <- debit_quota(user.submit_quota, bill.quota_debit),
         {:ok, reservation} <- opener.(admission, clock) do
      user = %{user | balance_minor: balance_minor, submit_quota: submit_quota}

      {:ok,
       %{
         state
         | users: Map.put(state.users, user.uid, user),
           reservations: Map.put(state.reservations, bill.bill_id, reservation)
       }}
    end
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
      Reservation.segment_mode?(reservation) ->
        {:error, :billing_conflict}

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

  defp expire_batch([], state), do: {:ok, state, 0}

  defp expire_batch(due, state) do
    case Enum.reduce_while(due, {:ok, state}, &expire_step/2) do
      {:ok, next} -> {:ok, next, length(due)}
      error -> error
    end
  end

  defp expire_step({_bill_id, reservation}, {:ok, state}) do
    case close(state, reservation, :expired, reservation.refundable_minor) do
      {:ok, next} -> {:cont, {:ok, next}}
      error -> {:halt, error}
    end
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
       when is_integer(balance) and is_integer(amount) and amount >= 0 do
    sum = balance + amount
    if sum <= @max_int64, do: {:ok, sum}, else: {:error, :amount_overflow}
  end

  defp credit_quota(nil, _amount), do: {:ok, nil}

  defp credit_quota(quota, amount)
       when is_integer(quota) and is_integer(amount) and amount >= 0 do
    sum = quota + amount
    if sum <= @max_int64, do: {:ok, sum}, else: {:error, :amount_overflow}
  end

  defp late_segment(%Tombstone{fingerprint: fingerprint}, fingerprint), do: {:ok, :late_ignored}
  defp late_segment(_stone, _fingerprint), do: {:error, :billing_conflict}

  defp apply_segment(state, reservation, fingerprint, index, outcome) do
    case SegmentLedger.record(
           reservation.ledger,
           reservation.bill_id,
           fingerprint,
           index,
           outcome
         ) do
      {:ok, :duplicate, _ledger, _delta} ->
        {:ok, :duplicate}

      {:ok, ledger, delta} ->
        credit_segment(state, reservation, ledger, delta, outcome)

      error ->
        error
    end
  end

  defp credit_segment(state, reservation, ledger, delta, outcome) do
    with {:ok, refundable} <- take_remainder(reservation, outcome),
         {:ok, user} <- fetch_user(state, reservation.uid),
         {:ok, balance_minor} <- credit_balance(user.balance_minor, delta.refund_minor),
         {:ok, submit_quota} <- credit_quota(user.submit_quota, delta.quota_credit) do
      reservation = %{reservation | ledger: ledger, refundable_minor: refundable}
      user = %{user | balance_minor: balance_minor, submit_quota: submit_quota}

      next = %{
        state
        | users: Map.put(state.users, user.uid, user),
          reservations: Map.put(state.reservations, reservation.bill_id, reservation)
      }

      close_if_terminal(next, reservation)
    end
  end

  defp take_remainder(reservation, :uncertain), do: {:ok, reservation.refundable_minor}

  defp take_remainder(reservation, _outcome) do
    count = reservation.ledger.count
    unit = div(reservation.reserved_minor, count)
    refundable = reservation.refundable_minor - unit

    if rem(reservation.reserved_minor, count) == 0 and refundable >= 0 do
      {:ok, refundable}
    else
      {:error, :invalid_bill}
    end
  end

  defp close_if_terminal(state, reservation) do
    if SegmentLedger.terminal?(reservation.ledger) do
      close(state, reservation, terminal_tombstone(reservation.ledger), 0)
    else
      {:ok, state}
    end
  end

  defp terminal_tombstone(%SegmentLedger{outcomes: outcomes}) do
    if Enum.any?(outcomes, fn {_index, outcome} -> outcome == :rejected end) do
      :settled_non_ok
    else
      :settled_ok
    end
  end
end
