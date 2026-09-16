defmodule JasminEx.Routing.Router do
  @moduledoc false
  use GenServer

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
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

  @spec settle(GenServer.server(), term()) ::
          {:ok, Tombstone.t()} | {:ok, :duplicate | :late_ignored} | {:error, atom()}
  def settle(server, settlement), do: GenServer.call(server, {:settle, settlement})

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

    case Snapshot.restore(config) do
      {:ok, state} ->
        Telemetry.emit([:snapshot], %{}, %{outcome: :ok})
        {:ok, state}

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
    mutate(state, fn -> admit_change(state, admission) end)
  end

  def handle_call({:settle, settlement}, _from, state) do
    mutate(state, fn -> settle_change(state, settlement) end)
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

  defp admit_change(state, %Admission{bill: %Bill{bill_id: bill_id}} = admission) do
    case {Map.get(state.tombstones, bill_id), Map.get(state.reservations, bill_id)} do
      {%Tombstone{} = stone, _} ->
        duplicate_admission(Tombstone.classify(stone, admission))

      {_, %Reservation{} = reservation} ->
        duplicate_admission(Reservation.classify(reservation, admission))

      {nil, nil} ->
        admit_new(state, admission, bill_id)
    end
  end

  defp admit_change(state, admission), do: State.admit(state, admission, clock())

  defp admit_new(state, admission, bill_id) do
    with {:ok, next} <- State.admit(state, admission, clock()),
         {:ok, reservation} <- fetch_admitted_reservation(next, bill_id) do
      {:ok, next, reservation}
    end
  end

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

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
