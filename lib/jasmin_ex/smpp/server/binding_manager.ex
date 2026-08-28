defmodule JasminEx.Smpp.Server.BindingManager do
  @moduledoc false
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.take(opts, [:name]))
  end

  def register(server, system_id, role, pid, limit) do
    GenServer.call(server, {:register, system_id, role, pid, limit})
  end

  def release(server, pid), do: GenServer.call(server, {:release, pid})
  def count(server, system_id), do: GenServer.call(server, {:count, system_id})

  @impl true
  def init(_opts), do: {:ok, %{by_pid: %{}, counts: %{}}}

  @impl true
  def handle_call({:register, system_id, role, pid, limit}, _from, state)
      when is_integer(limit) and limit >= 0 and is_pid(pid) and is_binary(system_id) do
    cond do
      Map.has_key?(state.by_pid, pid) ->
        {:reply, {:error, :duplicate_pid}, state}

      Map.get(state.counts, system_id, 0) >= limit ->
        {:reply, {:error, :max_bindings_exceeded}, state}

      true ->
        entry = %{system_id: system_id, role: role, ref: Process.monitor(pid)}
        counts = Map.update(state.counts, system_id, 1, &(&1 + 1))
        {:reply, :ok, %{state | by_pid: Map.put(state.by_pid, pid, entry), counts: counts}}
    end
  end

  def handle_call({:register, _system_id, _role, _pid, _limit}, _from, state),
    do: {:reply, {:error, :max_bindings_exceeded}, state}

  def handle_call({:release, pid}, _from, state), do: {:reply, :ok, drop(state, pid)}

  def handle_call({:count, system_id}, _from, state),
    do: {:reply, Map.get(state.counts, system_id, 0), state}

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state), do: {:noreply, drop(state, pid)}

  defp drop(state, pid) do
    case Map.pop(state.by_pid, pid) do
      {nil, _by_pid} ->
        state

      {%{system_id: system_id, ref: ref}, by_pid} ->
        Process.demonitor(ref, [:flush])
        counts = Map.update!(state.counts, system_id, &(&1 - 1))
        counts = if counts[system_id] == 0, do: Map.delete(counts, system_id), else: counts
        %{state | by_pid: by_pid, counts: counts}
    end
  end
end
