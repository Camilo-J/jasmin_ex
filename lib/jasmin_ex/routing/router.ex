defmodule JasminEx.Routing.Router do
  @moduledoc false
  use GenServer

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
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

  defp admit_change(state, %Admission{bill: %Bill{bill_id: bill_id}} = admission) do
    case Map.get(state.reservations, bill_id) do
      nil ->
        admit_new(state, admission, bill_id)

      reservation ->
        case Reservation.classify(reservation, admission) do
          {:ok, :duplicate, bill, fingerprint} ->
            {:unchanged, {:ok, :duplicate, bill, fingerprint}}

          {:error, reason} ->
            {:error, reason}
        end
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
