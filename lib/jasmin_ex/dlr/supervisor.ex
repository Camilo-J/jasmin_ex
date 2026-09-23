defmodule JasminEx.Dlr.Supervisor do
  @moduledoc false
  use Supervisor

  alias JasminEx.Dlr.Config
  alias JasminEx.Messaging.RabbitMQ.TopicPublisher

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name_opts(opts))

  @spec validate_options!(keyword()) :: keyword()
  def validate_options!(opts) when is_list(opts) do
    with %Config{enabled: true} <- Keyword.get(opts, :config),
         {store_module, _store_context} when is_atom(store_module) <- Keyword.get(opts, :store),
         connection when not is_nil(connection) <- Keyword.get(opts, :connection_server),
         {publisher_module, _publisher_context} when is_atom(publisher_module) <-
           Keyword.get(opts, :publisher) do
      opts
    else
      _invalid -> raise ArgumentError, "invalid enabled DLR dependencies"
    end
  end

  @impl true
  def init(opts) do
    validate_options!(opts)

    publisher = Keyword.fetch!(opts, :publisher)

    publisher_children =
      case publisher do
        {TopicPublisher, TopicPublisher} ->
          [
            {TopicPublisher,
             [
               config: Keyword.fetch!(opts, :messaging_config),
               connection_server: Keyword.fetch!(opts, :connection_server)
             ]}
          ]

        _injected ->
          []
      end

    Supervisor.init(
      publisher_children ++ [{JasminEx.Dlr.Readiness, Keyword.put(opts, :owner, self())}],
      strategy: :one_for_one
    )
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> []
      name -> [name: name]
    end
  end
end

defmodule JasminEx.Dlr.Readiness do
  @moduledoc false
  use GenServer

  require Logger

  alias JasminEx.Dlr.{HttpJob, HttpThrower, LookupPlan, Worker}
  alias JasminEx.Messaging.RabbitMQ.{Client, Connection, ConnectorWorker, TopicTopology}

  @initial_backoff_ms 250
  @max_backoff_ms 5_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  def status(server \\ __MODULE__), do: GenServer.call(server, :status)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    send(self(), :declare)

    {:ok,
     %{
       opts: opts,
       channel: nil,
       monitor: nil,
       backoff_ms: @initial_backoff_ms,
       ready: false,
       error: nil
     }}
  end

  @impl true
  def handle_call(:status, _from, state),
    do: {:reply, %{ready: state.ready, error: state.error}, state}

  @impl true
  def handle_info(:declare, %{ready: true} = state), do: {:noreply, state}
  def handle_info(:declare, state), do: {:noreply, declare(state)}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor: ref} = state) do
    stop_workers(state.opts)
    send(self(), :declare)
    {:noreply, %{state | ready: false, channel: nil, monitor: nil, error: :disconnected}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.monitor, do: Process.demonitor(state.monitor, [:flush])
    if state.channel, do: safe_close(state.channel)
  end

  defp declare(state) do
    opts = state.opts

    with {:ok, connection} <- Connection.get(Keyword.fetch!(opts, :connection_server)),
         {:ok, channel} <- Client.open_channel(connection) do
      case declare_channel(channel, opts) do
        :ok ->
          start_ready(state, channel)

        {:error, reason} ->
          safe_close(channel)
          retry(state, reason)
      end
    else
      {:error, reason} -> retry(state, reason)
    end
  catch
    :exit, reason -> retry(state, {:broker, reason})
  end

  defp start_ready(state, channel) do
    case start_workers(state.opts) do
      :ok ->
        %{
          state
          | channel: channel,
            monitor: Process.monitor(channel.pid),
            ready: true,
            error: nil,
            backoff_ms: @initial_backoff_ms
        }

      {:error, reason} ->
        safe_close(channel)
        retry(state, reason)
    end
  end

  defp declare_channel(channel, opts) do
    TopicTopology.declare(channel,
      prefix: Keyword.fetch!(opts, :config).queue_prefix,
      config: Keyword.fetch!(opts, :config)
    )
  catch
    :exit, reason -> {:error, TopicTopology.classify_declaration_failure(reason)}
  end

  defp retry(state, reason) do
    Logger.warning("DLR topology unavailable: #{inspect(reason)}")
    Process.send_after(self(), :declare, state.backoff_ms)
    %{state | ready: false, error: reason, backoff_ms: min(state.backoff_ms * 2, @max_backoff_ms)}
  end

  defp start_workers(opts) do
    names = TopicTopology.names(Keyword.fetch!(opts, :config).queue_prefix)
    owner = Keyword.fetch!(opts, :owner)
    publisher = Keyword.fetch!(opts, :publisher)
    store = Keyword.fetch!(opts, :store)
    clock = {ConnectorWorker, :system}

    publish_job = fn job ->
      with {:ok, payload} <- HttpJob.encode(job),
           {module, context} <- publisher do
        module.publish(context, "dlr_thrower.http", payload)
      end
    end

    client =
      case Keyword.fetch!(opts, :http_client) do
        {JasminEx.Dlr.HttpClient.Mint, client_opts} ->
          {JasminEx.Dlr.HttpClient.Mint,
           Keyword.put_new(client_opts, :timeout, opts[:config].http_timeout_ms)}

        injected ->
          injected
      end

    specs = [
      {Worker,
       [
         id: :dlr_lookup_worker,
         name: nil,
         queue_kind: :lookup,
         additional_attempts: opts[:config].lookup_additional_attempts,
         queue: names.lookup,
         connection_server: opts[:connection_server],
         processor: {LookupPlan, :process, [store: store, clock: clock, publisher: publish_job]}
       ]},
      {Worker,
       [
         id: :dlr_http_worker,
         name: nil,
         queue_kind: :http,
         additional_attempts: opts[:config].http_additional_attempts,
         queue: names.http,
         connection_server: opts[:connection_server],
         processor: {HttpThrower, :process, [clock: clock, client: client]}
       ]}
    ]

    Enum.reduce_while(specs, :ok, fn spec, :ok ->
      case Supervisor.start_child(owner, spec) do
        {:ok, _pid} ->
          {:cont, :ok}

        {:error, reason} ->
          stop_workers(opts)
          {:halt, {:error, reason}}
      end
    end)
  end

  defp stop_workers(opts) do
    owner = Keyword.fetch!(opts, :owner)

    Enum.each([:dlr_lookup_worker, :dlr_http_worker], fn id ->
      _ = Supervisor.terminate_child(owner, id)
      _ = Supervisor.delete_child(owner, id)
    end)
  end

  defp safe_close(channel) do
    if Process.alive?(channel.pid), do: Client.close_channel(channel)
  catch
    :exit, _ -> :ok
  end
end
