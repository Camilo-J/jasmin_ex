defmodule JasminEx.MtSubmitPipeline.ConcatReference do
  @moduledoc false
  use GenServer

  @type concat_reference :: 1..255

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, :ok, name: name)
  end

  @spec next(GenServer.server()) :: {:ok, concat_reference()} | {:error, :unavailable}
  def next(server) do
    {:ok, GenServer.call(server, :next)}
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  @impl true
  def init(:ok), do: {:ok, 0}

  @impl true
  def handle_call(:next, _from, reference) do
    next = if reference == 255, do: 1, else: reference + 1
    {:reply, next, next}
  end
end
