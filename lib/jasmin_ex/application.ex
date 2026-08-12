defmodule JasminEx.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias JasminEx.Messaging.RabbitMQ.Config, as: MessagingConfig
  alias JasminEx.Messaging.RabbitMQ.Supervisor, as: MessagingSupervisor
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.StateStore.Config

  @state_store_connection JasminEx.StateStore.Connection

  @spec children(keyword()) :: list()
  def children(config) do
    [state_store_child(Keyword.get(config, :state_store, []))] ++
      messaging_children(Keyword.get(config, :messaging, [])) ++
      smpp_children(config)
  end

  @impl true
  def start(_type, _args) do
    children =
      children(
        state_store: Application.get_env(:jasmin_ex, :state_store, []),
        messaging: Application.get_env(:jasmin_ex, :messaging, []),
        smpp_connectors: Application.get_env(:jasmin_ex, :smpp_connectors, [])
      )

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: JasminEx.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp state_store_child(options) do
    config = Config.new!(options)

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

  defp messaging_children(options) when is_list(options) do
    if Keyword.get(options, :enabled, false) do
      config = MessagingConfig.new!(options)
      [{MessagingSupervisor, [config: config]}]
    else
      []
    end
  end

  defp smpp_children(config) do
    case Keyword.get(config, :smpp_connectors, []) do
      [] -> []
      connectors -> [{ConnectorSupervisor, connectors}]
    end
  end
end
