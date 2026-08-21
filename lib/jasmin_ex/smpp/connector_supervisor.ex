defmodule JasminEx.Smpp.ConnectorSupervisor do
  @moduledoc """
  Supervises isolated SMPP connector subtrees with a one-for-one strategy.

  Each connector owns its client's restart-intensity budget, so one connector
  exhausting its budget does not terminate sibling connectors or this
  supervisor.
  """

  use Supervisor

  alias JasminEx.Smpp.Client.Config
  alias JasminEx.Smpp.ConnectorSupervisor.Instance

  @invalid_connectors_message "invalid SMPP connector configuration: expected [], one non-empty keyword connector configuration, or a list of non-empty keyword connector configurations"

  @spec start_link(keyword() | [keyword()]) :: Supervisor.on_start()
  def start_link(connectors), do: Supervisor.start_link(__MODULE__, connectors)

  @impl true
  def init(connectors) do
    children =
      connectors
      |> normalize_connectors()
      |> Enum.map(fn opts ->
        %{
          id: {:smpp_connector, Config.new!(opts).connector_id},
          start: {Instance, :start_link, [opts]},
          restart: :transient,
          type: :supervisor,
          modules: [Instance]
        }
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp normalize_connectors([]), do: []

  defp normalize_connectors(connectors) do
    cond do
      non_empty_keyword?(connectors) -> [connectors]
      connector_list?(connectors) -> connectors
      true -> raise ArgumentError, @invalid_connectors_message
    end
  end

  defp connector_list?([connector | connectors]),
    do: non_empty_keyword?(connector) and connector_list_tail?(connectors)

  defp connector_list?(_connectors), do: false

  defp connector_list_tail?([]), do: true

  defp connector_list_tail?([connector | connectors]),
    do: non_empty_keyword?(connector) and connector_list_tail?(connectors)

  defp connector_list_tail?(_connectors), do: false

  defp non_empty_keyword?([_head | _tail] = value), do: Keyword.keyword?(value)
  defp non_empty_keyword?(_value), do: false
end

defmodule JasminEx.Smpp.ConnectorSupervisor.LifecycleForwarder do
  @moduledoc false
  use GenServer

  def name(id), do: :"jasmin_ex.lifecycle_forwarder.#{id}"
  def worker_name(id), do: :"jasmin_ex.connector_worker.#{id}"

  def start_link(id, owner),
    do: GenServer.start_link(__MODULE__, {id, owner}, name: name(id))

  def locate(id, _owner) do
    case Process.whereis(name(id)) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> {:error, :not_started}
    end
  end

  def init({id, owner}) do
    worker =
      case Process.whereis(worker_name(id)) do
        pid when is_pid(pid) ->
          Process.monitor(pid)
          pid

        nil ->
          nil
      end

    {:ok, {Process.monitor(owner), worker, []}}
  end

  def handle_info({:worker, w}, {o, _, b}) do
    Enum.each(b, &send(w, &1))
    Process.monitor(w)
    {:noreply, {o, w, b}}
  end

  def handle_info({:DOWN, r, :process, _, _}, {r, _, _} = s), do: {:stop, :normal, s}
  def handle_info({:DOWN, _, :process, w, _}, {o, w, b}), do: {:noreply, {o, nil, b}}
  def handle_info({:DOWN, _, :process, _, _}, s), do: {:noreply, s}

  def handle_info(m, {o, w, _}) do
    if is_pid(w), do: send(w, m)
    bound = if match?({:smpp_bound, _, _}, m), do: [m], else: []
    {:noreply, {o, w, bound}}
  end
end

defmodule JasminEx.Smpp.ConnectorSupervisor.Instance do
  @moduledoc false

  use Supervisor

  alias JasminEx.Messaging.RabbitMQ.Config, as: MessagingConfig
  alias JasminEx.Messaging.RabbitMQ.Connection
  alias JasminEx.Messaging.RabbitMQ.ConnectorWorker
  alias JasminEx.Messaging.RabbitMQ.WorkQueue
  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.Client.Config
  alias JasminEx.Smpp.ConnectorSupervisor.LifecycleForwarder
  alias JasminEx.StateStore.Config, as: StateStoreConfig
  alias JasminEx.StateStore.Redix, as: StateStoreRedix

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  def start_client(opts, id) do
    Client.start_link(Keyword.put(opts, :lifecycle_notify, LifecycleForwarder.name(id)))
  end

  def start_worker(opts, id) do
    supervisor = self()

    dependencies = [
      store: state_store(),
      submit: fn envelope ->
        Client.send_submit_sm(
          child(supervisor, :smpp_client),
          struct(JasminEx.Smpp.PDU.Body.SubmitSM, envelope.submit_sm)
        )
      end,
      republish: &WorkQueue.republish/1
    ]

    case ConnectorWorker.start_link(Keyword.merge(opts, dependencies)) do
      {:ok, worker} = ok ->
        case LifecycleForwarder.locate(id, supervisor) do
          {:ok, forwarder} ->
            send(forwarder, {:worker, worker})
            ok

          {:error, reason} ->
            GenServer.stop(worker)
            {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @impl true
  def init(opts) do
    {messaging, client_opts} = Keyword.pop(opts, :messaging, messaging_config())

    Supervisor.init(
      forwarder_children(messaging, client_opts) ++
        [client_child(client_opts, messaging) | worker_children(messaging, client_opts)],
      strategy: :one_for_one,
      max_restarts: 3,
      max_seconds: 5
    )
  end

  defp forwarder_children(messaging, opts) when is_list(messaging) do
    if Keyword.get(messaging, :enabled, false) do
      [
        %{
          id: :lifecycle_forwarder,
          start: {LifecycleForwarder, :start_link, [Config.new!(opts).connector_id, self()]},
          restart: :permanent,
          type: :worker,
          modules: [LifecycleForwarder]
        }
      ]
    else
      []
    end
  end

  defp forwarder_children(_messaging, _opts), do: []

  defp client_child(opts, messaging) do
    start =
      if is_list(messaging) and Keyword.get(messaging, :enabled, false) do
        {__MODULE__, :start_client, [opts, Config.new!(opts).connector_id]}
      else
        {Client, :start_link, [opts]}
      end

    %{
      id: :smpp_client,
      start: start,
      restart: :transient,
      type: :worker,
      modules: [Client]
    }
  end

  defp worker_children(messaging, opts) when is_list(messaging) do
    if Keyword.get(messaging, :enabled, false) do
      [worker_child(messaging, opts)]
    else
      []
    end
  end

  defp worker_children(_messaging, _opts), do: []

  defp worker_child(messaging, opts) do
    connector_id = Config.new!(opts).connector_id

    %{
      id: :connector_worker,
      start:
        {__MODULE__, :start_worker,
         [
           [
             config: MessagingConfig.new!(messaging),
             connector_id: connector_id,
             connection_server: Connection,
             name: LifecycleForwarder.worker_name(connector_id)
           ],
           connector_id
         ]},
      restart: :transient,
      type: :worker,
      modules: [ConnectorWorker]
    }
  end

  defp messaging_config, do: Application.get_env(:jasmin_ex, :messaging, [])

  defp state_store do
    config = StateStoreConfig.new!(Application.get_env(:jasmin_ex, :state_store, []))
    {StateStoreRedix, StateStoreRedix.context(config)}
  end

  defp child(supervisor, id) do
    {^id, pid, _, _} = Enum.find(Supervisor.which_children(supervisor), &(elem(&1, 0) == id))
    pid
  end
end
