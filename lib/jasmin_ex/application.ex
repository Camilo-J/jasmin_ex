defmodule JasminEx.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias JasminEx.Dlr.Config, as: DlrConfig
  alias JasminEx.Dlr.Event, as: DlrEvent
  alias JasminEx.Dlr.Supervisor, as: DlrSupervisor
  alias JasminEx.HttpApi
  alias JasminEx.Messaging.RabbitMQ.Config, as: MessagingConfig
  alias JasminEx.Messaging.RabbitMQ.Connection, as: MessagingConnection
  alias JasminEx.Messaging.RabbitMQ.Supervisor, as: MessagingSupervisor
  alias JasminEx.Messaging.RabbitMQ.TopicPublisher
  alias JasminEx.Routing.Config, as: RoutingConfig
  alias JasminEx.Routing.Router
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.Server
  alias JasminEx.StateStore.Config, as: StateStoreConfig
  alias JasminEx.StateStore.Redix, as: StateStore

  @state_store_connection JasminEx.StateStore.Connection

  @spec children(keyword()) :: list()
  def children(config) do
    state_store_config = StateStoreConfig.new!(Keyword.get(config, :state_store, []))
    messaging_options = Keyword.get(config, :messaging, [])
    dlr_options = dlr_options(config, state_store_config, messaging_options)

    [state_store_child(state_store_config)] ++
      [routing_child(Keyword.get(config, :routing, []))] ++
      messaging_children(messaging_options) ++
      dlr_children(dlr_options) ++
      smpp_children(config, dlr_options) ++
      smpp_server_children(config) ++
      http_api_children(config, dlr_options)
  end

  @impl true
  def start(_type, _args) do
    children =
      children(
        state_store: Application.get_env(:jasmin_ex, :state_store, []),
        routing: Application.get_env(:jasmin_ex, :routing, []),
        messaging: Application.get_env(:jasmin_ex, :messaging, []),
        dlr: Application.get_env(:jasmin_ex, :dlr, []),
        smpp_connectors: Application.get_env(:jasmin_ex, :smpp_connectors, []),
        smpp_server: Application.get_env(:jasmin_ex, :smpp_server, []),
        http_api: Application.get_env(:jasmin_ex, :http_api, [])
      )

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: JasminEx.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp state_store_child(config) do
    %{
      id: @state_store_connection,
      start: {Redix, :start_link, [redix_options(config)]}
    }
  end

  defp redix_options(config) do
    [
      host: config.host,
      port: config.port,
      database: config.database,
      username: config.username,
      password: config.password,
      name: @state_store_connection,
      sync_connect: false,
      exit_on_disconnection: false,
      backoff_initial: config.backoff_initial_ms,
      backoff_max: config.backoff_max_ms,
      timeout: config.connect_timeout_ms,
      health_check_interval: config.health_check_timeout_ms,
      ssl: config.tls
    ]
  end

  defp routing_child(options) when is_list(options) do
    {Router, [config: RoutingConfig.new(options), name: Keyword.get(options, :name, Router)]}
  end

  defp messaging_children(options) when is_list(options) do
    if Keyword.get(options, :enabled, false) do
      config = MessagingConfig.new!(options)
      [{MessagingSupervisor, [config: config]}]
    else
      []
    end
  end

  defp dlr_options(config, state_store_config, messaging_options) do
    options = Keyword.get(config, :dlr, [])

    case Keyword.get(options, :enabled, false) do
      false ->
        []

      true ->
        require_messaging!(messaging_options)
        dlr_config = dlr_config!(options)

        [
          config: dlr_config,
          store:
            Keyword.get(options, :store, {StateStore, StateStore.context(state_store_config)}),
          connection_server: Keyword.get(options, :connection_server, MessagingConnection),
          publisher: Keyword.get(options, :publisher, {TopicPublisher, TopicPublisher}),
          messaging_config: MessagingConfig.new!(messaging_options),
          http_client: Keyword.get(options, :http_client, {JasminEx.Dlr.HttpClient.Mint, []})
        ]
        |> DlrSupervisor.validate_options!()

      _invalid ->
        raise ArgumentError, "invalid DLR configuration"
    end
  end

  defp dlr_config!(options) do
    case DlrConfig.new(options) do
      %DlrConfig{} = config -> config
      {:error, :invalid_dlr_config} -> raise ArgumentError, "invalid DLR configuration"
    end
  end

  defp require_messaging!(options) do
    unless Keyword.get(options, :enabled, false) == true do
      raise ArgumentError, "enabled DLR requires enabled RabbitMQ messaging"
    end
  end

  defp dlr_children([]), do: []
  defp dlr_children(options), do: [{DlrSupervisor, options}]

  defp smpp_children(config, dlr_options) do
    case Keyword.get(config, :smpp_connectors, []) do
      [] -> []
      connectors when dlr_options == [] -> [{ConnectorSupervisor, connectors}]
      connectors -> [{ConnectorSupervisor, inject_connectors(connectors, dlr_options)}]
    end
  end

  defp inject_connectors(connectors, dlr_options) do
    publisher = Keyword.fetch!(dlr_options, :publisher)
    expiry = Keyword.fetch!(dlr_options, :config).dlr_expiry_s

    known_publisher = fn key, payload ->
      with "dlr.submit_sm_resp" <- key,
           {:ok, event} <- known_event(payload, expiry),
           {:ok, encoded} <- DlrEvent.encode(event),
           {module, context} <- publisher do
        module.publish(context, key, encoded)
      else
        _ -> {:error, :invalid_dlr_event}
      end
    end

    inject = fn
      opts when is_list(opts) ->
        opts
        |> Keyword.put_new(:dlr_enabled, true)
        |> Keyword.put_new(:dlr_expiry, expiry)
        |> Keyword.put_new(:dlr_publisher, publisher)
        |> Keyword.put_new(:dlr_known_publisher, known_publisher)

      opts when is_map(opts) ->
        opts
        |> Map.put_new(:dlr_enabled, true)
        |> Map.put_new(:dlr_expiry, expiry)
        |> Map.put_new(:dlr_publisher, publisher)
        |> Map.put_new(:dlr_known_publisher, known_publisher)
    end

    if Keyword.keyword?(connectors), do: inject.(connectors), else: Enum.map(connectors, inject)
  end

  defp known_event(payload, expiry) do
    known = :json.decode(payload)
    now = known["observed_at_ms"]

    {:ok,
     %{
       kind: :submit_sm_resp,
       gateway_id: known["gateway_id"],
       connector_id: known["connector_id"],
       attempt: known["attempt"],
       status: known["status"],
       raw_smsc_id: known["smsc_id"],
       observed_at_ms: now,
       deadline_ms: now + expiry * 1000
     }}
  rescue
    _ -> {:error, :invalid_dlr_event}
  end

  defp smpp_server_children(config) do
    options = Keyword.get(config, :smpp_server, [])
    server = Server.Config.new(options)

    if server.enabled do
      [{Server.Supervisor, [config: server, router: Keyword.get(options, :router, Router)]}]
    else
      []
    end
  end

  defp http_api_children(config, dlr_options) do
    options = Keyword.get(config, :http_api, [])
    http = HttpApi.Config.new(options)

    if http.enabled do
      [
        {HttpApi.Supervisor,
         [
           config: http,
           router: Keyword.get(options, :router, Router),
           queue: Keyword.get(options, :queue)
         ] ++ http_dlr_dependencies(dlr_options)}
      ]
    else
      []
    end
  end

  defp http_dlr_dependencies([]), do: []

  defp http_dlr_dependencies(dlr_options) do
    [
      dlr_store: Keyword.fetch!(dlr_options, :store),
      dlr_config: Keyword.fetch!(dlr_options, :config)
    ]
  end
end
