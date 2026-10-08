defmodule JasminEx.Routing.Router do
  @moduledoc false
  use GenServer

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.SegmentDispatch
  alias JasminEx.Billing.Settlement
  alias JasminEx.Billing.Tombstone
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.Snapshot
  alias JasminEx.Routing.State
  alias JasminEx.Routing.Telemetry
  alias JasminEx.Routing.User

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name)))
  end

  @spec snapshot(GenServer.server()) :: State.t()
  def snapshot(server), do: GenServer.call(server, :snapshot)

  @spec put_group(GenServer.server(), keyword()) :: {:ok, Group.t()} | {:error, atom()}
  def put_group(server, attrs), do: GenServer.call(server, {:put_group, attrs})

  @spec put_user(GenServer.server(), keyword()) :: {:ok, User.t()} | {:error, atom()}
  def put_user(server, attrs), do: GenServer.call(server, {:put_user, attrs})

  @spec put_route(GenServer.server(), keyword()) :: {:ok, Route.t()} | {:error, atom()}
  def put_route(server, attrs), do: GenServer.call(server, {:put_route, attrs})

  @spec delete_group(GenServer.server(), String.t()) :: {:ok, String.t()} | {:error, atom()}
  def delete_group(server, gid), do: GenServer.call(server, {:delete_group, gid})

  @spec admit(GenServer.server(), term()) ::
          {:ok, Reservation.t()}
          | {:ok, :duplicate, Bill.t(), Fingerprint.t()}
          | {:error, atom()}
  def admit(server, admission), do: GenServer.call(server, {:admit, admission})

  @spec admit_segments(GenServer.server(), term()) ::
          {:ok, Reservation.t()}
          | {:ok, :duplicate, Bill.t(), Fingerprint.t()}
          | {:error, atom()}
  def admit_segments(server, admission), do: GenServer.call(server, {:admit_segments, admission})

  @spec admit_segments_with_dispatch(GenServer.server(), term(), term()) ::
          {:ok, Reservation.t(), reference()} | {:ok, :duplicate} | {:error, atom()}
  def admit_segments_with_dispatch(server, admission, children) do
    GenServer.call(server, {:admit_segments_with_dispatch, admission, children})
  end

  @spec claim_segment_dispatch(GenServer.server(), term(), term(), term()) ::
          {:ok, SegmentDispatch.t()} | {:error, atom()}
  def claim_segment_dispatch(server, bill_id, generation, gateway_id) do
    GenServer.call(server, {:claim_segment_dispatch, bill_id, generation, gateway_id})
  end

  @spec record_segment_dispatch(GenServer.server(), term(), term(), term(), term()) ::
          {:ok, Reservation.t() | Tombstone.t()} | {:ok, :duplicate} | {:error, atom()}
  def record_segment_dispatch(server, bill_id, generation, gateway_id, outcome) do
    GenServer.call(server, {:record_segment_dispatch, bill_id, generation, gateway_id, outcome})
  end

  @spec recover_segment_dispatch(GenServer.server(), term()) ::
          {:ok, Reservation.t() | Tombstone.t()} | {:ok, :duplicate} | {:error, atom()}
  def recover_segment_dispatch(server, bill_id) do
    GenServer.call(server, {:recover_segment_dispatch, bill_id})
  end

  @spec settle(GenServer.server(), term()) ::
          {:ok, Tombstone.t()} | {:ok, :duplicate | :late_ignored} | {:error, atom()}
  def settle(server, settlement), do: GenServer.call(server, {:settle, settlement})

  @spec settle_segment(GenServer.server(), term(), term(), term(), term()) ::
          {:ok, Reservation.t() | Tombstone.t()}
          | {:ok, :duplicate | :late_ignored}
          | {:error, atom()}
  def settle_segment(server, bill_id, fingerprint, index, outcome) do
    GenServer.call(server, {:settle_segment, bill_id, fingerprint, index, outcome})
  end

  @spec expire_due(GenServer.server()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def expire_due(server), do: GenServer.call(server, :expire_due)

  @spec set_balance(GenServer.server(), term(), term()) :: {:ok, User.t()} | {:error, atom()}
  def set_balance(server, uid, amount), do: GenServer.call(server, {:set_balance, uid, amount})

  @spec set_quota(GenServer.server(), term(), term()) :: {:ok, User.t()} | {:error, atom()}
  def set_quota(server, uid, amount), do: GenServer.call(server, {:set_quota, uid, amount})

  def set_smpp_secret(server, uid, secret),
    do: GenServer.call(server, {:set_smpp_secret, uid, secret})

  def set_max_bindings(server, uid, limit),
    do: GenServer.call(server, {:set_max_bindings, uid, limit})

  def set_dlr_level(server, uid, value),
    do: GenServer.call(server, {:set_dlr_level, uid, value})

  def set_http_set_dlr_method(server, uid, value),
    do: GenServer.call(server, {:set_http_set_dlr_method, uid, value})

  @spec set_rate(GenServer.server(), term(), term()) :: {:ok, Route.t()} | {:error, atom()}
  def set_rate(server, order, rate), do: GenServer.call(server, {:set_rate, order, rate})

  @impl true
  def init(opts) do
    config = Keyword.get(opts, :config) || Config.new()

    Process.put({__MODULE__, :config}, config)
    Process.put({__MODULE__, :owners}, %{})

    case Snapshot.restore(config) do
      {:ok, state} ->
        boot(state, config)

      {:error, reason} ->
        Telemetry.emit([:snapshot], %{}, %{outcome: :restore_failed})
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, state, state}

  def handle_call({:put_group, attrs}, _from, state) do
    mutate(state, fn ->
      with {:ok, group} <- Group.new(attrs),
           {:ok, next} <- State.put_group(state, group),
           do: {:ok, next, group}
    end)
  end

  def handle_call({:put_user, attrs}, _from, state) do
    mutate(state, fn ->
      with {:ok, user} <- User.new(attrs),
           {:ok, next} <- State.put_user(state, user),
           do: {:ok, next, user}
    end)
  end

  def handle_call({:put_route, attrs}, _from, state) do
    mutate(state, fn ->
      with {:ok, route} <- Route.new(attrs),
           {:ok, next} <- State.put_route(state, route),
           do: {:ok, next, route}
    end)
  end

  def handle_call({:delete_group, gid}, _from, state) do
    mutate(state, fn ->
      with {:ok, next} <- State.delete_group(state, gid), do: {:ok, next, gid}
    end)
  end

  def handle_call({:admit, admission}, _from, state) do
    mutate(state, fn -> admit_change(state, admission, :legacy) end)
  end

  def handle_call({:admit_segments, admission}, _from, state) do
    mutate(state, fn -> admit_change(state, admission, :segments) end)
  end

  def handle_call({:admit_segments_with_dispatch, admission, children}, {pid, _tag}, state) do
    case mutate(state, fn -> admit_dispatch_change(state, admission, children) end) do
      {:reply, {:ok, {reservation, dispatch}}, next} ->
        generation = take_ownership(dispatch.bill_id, pid)
        {:reply, {:ok, reservation, generation}, next}

      other ->
        other
    end
  end

  def handle_call({:claim_segment_dispatch, bill_id, generation, gateway_id}, {pid, _tag}, state) do
    case authorize_owner(bill_id, generation, pid) do
      :ok -> mutate(state, fn -> claim_change(state, bill_id, gateway_id) end)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:record_segment_dispatch, bill_id, generation, gateway_id, outcome},
        {pid, _tag},
        state
      ) do
    case authorize_owner(bill_id, generation, pid) do
      :ok ->
        finish_record(
          bill_id,
          mutate(state, fn -> record_change(state, bill_id, gateway_id, outcome) end)
        )

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:recover_segment_dispatch, bill_id}, _from, state) do
    case may_recover(bill_id) do
      :ok -> finish_recover(bill_id, mutate(state, fn -> recover_change(state, bill_id) end))
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:settle, settlement}, _from, state) do
    mutate(state, fn -> settle_change(state, settlement) end)
  end

  def handle_call({:settle_segment, bill_id, fingerprint, index, outcome}, _from, state) do
    mutate(state, fn -> settle_segment_change(state, bill_id, fingerprint, index, outcome) end)
  end

  def handle_call(:expire_due, _from, state) do
    mutate(state, fn -> expire_change(state) end)
  end

  def handle_call({:set_balance, uid, amount}, _from, state) do
    mutate_billing(state, fn -> wrap_admin(State.set_balance(state, uid, amount)) end)
  end

  def handle_call({:set_quota, uid, amount}, _from, state) do
    mutate_billing(state, fn -> wrap_admin(State.set_quota(state, uid, amount)) end)
  end

  def handle_call({:set_smpp_secret, uid, secret}, _from, state) do
    mutate(state, fn -> wrap_admin(State.set_smpp_secret(state, uid, secret)) end)
  end

  def handle_call({:set_max_bindings, uid, limit}, _from, state) do
    mutate(state, fn -> wrap_admin(State.set_max_bindings(state, uid, limit)) end)
  end

  def handle_call({:set_dlr_level, uid, value}, _from, state) do
    mutate(state, fn -> wrap_admin(State.set_dlr_level(state, uid, value)) end)
  end

  def handle_call({:set_http_set_dlr_method, uid, value}, _from, state) do
    mutate(state, fn -> wrap_admin(State.set_http_set_dlr_method(state, uid, value)) end)
  end

  def handle_call({:set_rate, order, rate}, _from, state) do
    mutate_billing(state, fn -> wrap_admin(State.set_rate(state, order, rate)) end)
  end

  def handle_call(request, _from, state)
      when is_tuple(request) and elem(request, 0) in [:set_balance, :set_quota, :set_rate] do
    {:reply, {:error, :invalid_amount}, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case owner_by_monitor(ref, pid) do
      {:ok, bill_id} -> recover_down(state, bill_id)
      :error -> {:noreply, state}
    end
  end

  defp admit_change(state, %Admission{bill: %Bill{bill_id: bill_id}} = admission, mode) do
    case {Map.get(state.tombstones, bill_id), Map.get(state.reservations, bill_id)} do
      {%Tombstone{} = stone, _} ->
        duplicate_admission(Tombstone.classify(stone, admission))

      {_, %Reservation{} = reservation} ->
        replay_admission(reservation, admission, mode)

      {nil, nil} ->
        admit_new(state, admission, bill_id, mode)
    end
  end

  defp admit_change(state, admission, :segments),
    do: State.admit_segments(state, admission, clock())

  defp admit_change(state, admission, _mode), do: State.admit(state, admission, clock())

  defp replay_admission(reservation, admission, mode) do
    if Reservation.segment_mode?(reservation) == (mode == :segments) do
      duplicate_admission(Reservation.classify(reservation, admission))
    else
      {:error, :billing_conflict}
    end
  end

  defp admit_new(state, admission, bill_id, mode) do
    with {:ok, next} <- open_admission(state, admission, mode),
         {:ok, reservation} <- fetch_admitted_reservation(next, bill_id) do
      {:ok, next, reservation}
    end
  end

  defp open_admission(state, admission, :segments),
    do: State.admit_segments(state, admission, clock())

  defp open_admission(state, admission, _mode), do: State.admit(state, admission, clock())

  defp fetch_admitted_reservation(next, bill_id) do
    case Map.fetch(next.reservations, bill_id) do
      {:ok, reservation} -> {:ok, reservation}
      :error -> {:error, :inconsistent_admission}
    end
  end

  defp duplicate_admission({:ok, :duplicate, bill, fingerprint}) do
    {:unchanged, {:ok, :duplicate, bill, fingerprint}}
  end

  defp duplicate_admission({:error, reason}), do: {:error, reason}

  defp settle_change(state, %Settlement{bill_id: bill_id} = settlement) do
    case State.settle(state, settlement) do
      {:ok, :duplicate} -> {:unchanged, {:ok, :duplicate}}
      {:ok, :late_ignored} -> {:unchanged, {:ok, :late_ignored}}
      {:ok, next} -> fetch_settled_tombstone(next, bill_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp settle_change(_state, _settlement), do: {:error, :invalid_bill_id}

  defp settle_segment_change(state, bill_id, fingerprint, index, outcome) do
    case State.settle_segment(state, bill_id, fingerprint, index, outcome) do
      {:ok, :duplicate} -> {:unchanged, {:ok, :duplicate}}
      {:ok, :late_ignored} -> {:unchanged, {:ok, :late_ignored}}
      {:ok, next} -> fetch_segment_result(next, bill_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_segment_result(next, bill_id) do
    cond do
      Map.has_key?(next.tombstones, bill_id) and not Map.has_key?(next.reservations, bill_id) ->
        {:ok, next, next.tombstones[bill_id]}

      Map.has_key?(next.reservations, bill_id) ->
        {:ok, next, next.reservations[bill_id]}

      true ->
        {:error, :inconsistent_settlement}
    end
  end

  defp fetch_settled_tombstone(next, bill_id) do
    case Map.fetch(next.tombstones, bill_id) do
      {:ok, stone} -> {:ok, next, stone}
      :error -> {:error, :inconsistent_settlement}
    end
  end

  defp expire_change(state) do
    case State.expire_due(state, clock()) do
      {:ok, _state, 0} -> {:unchanged, {:ok, 0}}
      {:ok, next, count} -> {:ok, next, count}
      {:error, reason} -> {:error, reason}
    end
  end

  defp wrap_admin({:unchanged, value}), do: {:unchanged, {:ok, value}}
  defp wrap_admin(other), do: other

  defp mutate_billing(state, fun) do
    case mutate(state, fun) do
      {:reply, {:ok, value}, %{revision: revision} = next} when revision != state.revision ->
        Telemetry.emit([:billing], %{count: 1, bytes: 0}, %{outcome: :ok})
        {:reply, {:ok, value}, next}

      other ->
        other
    end
  end

  defp mutate(state, fun) do
    case fun.() do
      {:ok, next, value} ->
        commit(state, publish(next), value)

      {:unchanged, reply} ->
        {:reply, reply, state}

      {:error, reason} ->
        emit_mutation(:error, reason, state.revision)
        {:reply, {:error, reason}, state}
    end
  end

  defp clock do
    %Config{clock: clock} = Process.get({__MODULE__, :config})
    clock
  end

  defp commit(state, published, value) do
    case Snapshot.write(published, Process.get({__MODULE__, :config})) do
      :ok ->
        emit_mutation(:ok, nil, published.revision)
        Telemetry.emit([:snapshot], %{}, %{outcome: :ok})
        {:reply, {:ok, value}, published}

      {:error, _reason} ->
        emit_mutation(:error, :snapshot_failed, state.revision)
        Telemetry.emit([:snapshot], %{}, %{outcome: :snapshot_failed})
        {:reply, {:error, :snapshot_failed}, state}
    end
  end

  defp emit_mutation(outcome, reason, revision) do
    Telemetry.emit([:mutation], %{}, %{outcome: outcome, reason: reason, revision: revision})
  end

  defp publish(state), do: %{state | revision: state.revision + 1}

  defp boot(state, config) do
    case State.recover_open_dispatches(state) do
      {:ok, :unchanged} ->
        Telemetry.emit([:snapshot], %{}, %{outcome: :ok})
        {:ok, state}

      {:ok, next} ->
        persist_boot(publish(next), config)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp persist_boot(published, config) do
    case Snapshot.write(published, config) do
      :ok ->
        Telemetry.emit([:snapshot], %{}, %{outcome: :ok})
        {:ok, published}

      {:error, _reason} ->
        Telemetry.emit([:snapshot], %{}, %{outcome: :snapshot_failed})
        {:stop, :snapshot_failed}
    end
  end

  defp admit_dispatch_change(
         state,
         %Admission{bill: %Bill{bill_id: bill_id}} = admission,
         children
       ) do
    case State.admit_segments_with_dispatch(state, admission, children, clock()) do
      {:ok, :duplicate} ->
        {:unchanged, {:ok, :duplicate}}

      {:ok, next} ->
        {:ok, next, {next.reservations[bill_id], next.segment_dispatches[bill_id]}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp admit_dispatch_change(_state, _admission, _children), do: {:error, :invalid_bill_id}

  defp claim_change(state, bill_id, gateway_id) do
    case State.claim_dispatch(state, bill_id, gateway_id) do
      {:ok, next} -> {:ok, next, next.segment_dispatches[bill_id]}
      error -> error
    end
  end

  defp record_change(state, bill_id, gateway_id, outcome) do
    case State.record_dispatch(state, bill_id, gateway_id, outcome) do
      {:ok, :duplicate} -> {:unchanged, {:ok, :duplicate}}
      {:ok, next} -> fetch_segment_result(next, bill_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp recover_change(state, bill_id) do
    case State.recover_dispatch(state, bill_id) do
      {:ok, :duplicate} -> {:unchanged, {:ok, :duplicate}}
      {:ok, next} -> fetch_segment_result(next, bill_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp finish_record(bill_id, {:reply, {:ok, value}, next}) do
    maybe_drop_closed(next, bill_id)
    {:reply, {:ok, value}, next}
  end

  defp finish_record(bill_id, {:reply, {:error, :snapshot_failed}, next}) do
    fence_owner(bill_id)
    {:reply, {:error, :snapshot_failed}, next}
  end

  defp finish_record(_bill_id, other), do: other

  defp finish_recover(bill_id, {:reply, {:ok, value}, next}) do
    drop_owner(bill_id)
    {:reply, {:ok, value}, next}
  end

  defp finish_recover(bill_id, {:reply, {:error, :snapshot_failed}, next}) do
    fence_owner(bill_id)
    {:reply, {:error, :snapshot_failed}, next}
  end

  defp finish_recover(_bill_id, other), do: other

  defp recover_down(state, bill_id) do
    case recover_change(state, bill_id) do
      {:unchanged, _reply} ->
        drop_owner(bill_id)
        {:noreply, state}

      {:ok, next, _value} ->
        commit_down(state, bill_id, publish(next))

      {:error, _reason} ->
        fence_owner(bill_id)
        {:noreply, state}
    end
  end

  defp commit_down(state, bill_id, published) do
    case Snapshot.write(published, Process.get({__MODULE__, :config})) do
      :ok ->
        emit_mutation(:ok, nil, published.revision)
        Telemetry.emit([:snapshot], %{}, %{outcome: :ok})
        drop_owner(bill_id)
        {:noreply, published}

      {:error, _reason} ->
        emit_mutation(:error, :snapshot_failed, state.revision)
        Telemetry.emit([:snapshot], %{}, %{outcome: :snapshot_failed})
        fence_owner(bill_id)
        {:noreply, state}
    end
  end

  defp take_ownership(bill_id, pid) do
    generation = make_ref()
    monitor = Process.monitor(pid)

    put_owners(
      Map.put(owners(), bill_id, %{
        pid: pid,
        monitor: monitor,
        generation: generation,
        fenced: false
      })
    )

    generation
  end

  defp authorize_owner(bill_id, generation, pid) do
    case Map.get(owners(), bill_id) do
      %{generation: ^generation, pid: ^pid, fenced: false} -> :ok
      %{generation: ^generation, pid: ^pid, fenced: true} -> {:error, :owner_fenced}
      %{generation: ^generation} -> {:error, :owner_mismatch}
      %{generation: _other} -> {:error, :generation_mismatch}
      nil -> {:error, :generation_mismatch}
    end
  end

  defp may_recover(bill_id) do
    case Map.get(owners(), bill_id) do
      %{pid: pid, fenced: false} ->
        if Process.alive?(pid), do: {:error, :owner_alive}, else: :ok

      _other ->
        :ok
    end
  end

  defp maybe_drop_closed(state, bill_id) do
    case Map.get(state.segment_dispatches, bill_id) do
      %SegmentDispatch{phase: :closed} -> drop_owner(bill_id)
      _other -> :ok
    end
  end

  defp drop_owner(bill_id) do
    case Map.pop(owners(), bill_id) do
      {%{monitor: monitor}, rest} ->
        Process.demonitor(monitor, [:flush])
        put_owners(rest)

      {nil, rest} ->
        put_owners(rest)
    end
  end

  defp fence_owner(bill_id) do
    case Map.get(owners(), bill_id) do
      nil -> :ok
      owner -> put_owners(Map.put(owners(), bill_id, %{owner | fenced: true}))
    end
  end

  defp owner_by_monitor(ref, pid) do
    Enum.find_value(owners(), :error, fn
      {bill_id, %{monitor: ^ref, pid: ^pid}} -> {:ok, bill_id}
      _other -> nil
    end)
  end

  defp owners, do: Process.get({__MODULE__, :owners}) || %{}
  defp put_owners(map), do: Process.put({__MODULE__, :owners}, map)

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
