defmodule JasminEx.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias JasminEx.Smpp.ConnectorSupervisor

  @spec children(keyword()) :: list()
  def children(config) do
    case Keyword.get(config, :smpp_connectors, []) do
      [] -> []
      connectors -> [{ConnectorSupervisor, connectors}]
    end
  end

  @impl true
  def start(_type, _args) do
    children = children(smpp_connectors: Application.get_env(:jasmin_ex, :smpp_connectors, []))

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: JasminEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
