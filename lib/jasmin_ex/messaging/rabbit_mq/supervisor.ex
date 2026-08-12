defmodule JasminEx.Messaging.RabbitMQ.Supervisor do
  @moduledoc false
  use Supervisor

  alias JasminEx.Messaging.RabbitMQ.{Connection, Publisher}

  def start_link(opts) when is_list(opts) do
    Supervisor.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name, __MODULE__)))
  end

  def child_spec(opts) when is_list(opts) do
    opts
    |> super()
    |> Map.put(:id, Keyword.get(opts, :id, __MODULE__))
  end

  @impl true
  def init(opts) when is_list(opts) do
    config = Keyword.fetch!(opts, :config)
    client = Keyword.get(opts, :client, JasminEx.Messaging.RabbitMQ.Client)
    client_opts = Keyword.get(opts, :client_opts, [])
    connection_name = Keyword.get(opts, :connection_name, Connection)
    publisher_name = Keyword.get(opts, :publisher_name, Publisher)

    children = [
      {Connection,
       [
         config: config,
         client: client,
         client_opts: client_opts,
         name: connection_name,
         id: Connection
       ]},
      {Publisher,
       [
         config: config,
         client: client,
         connection_server: connection_name,
         name: publisher_name,
         id: Publisher
       ]}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
