defmodule JasminEx.Smpp.ConnectorSupervisor do
  @moduledoc "Supervises SMPP client sessions independently."

  use Supervisor

  alias JasminEx.Smpp.Client

  @spec start_link(keyword() | [keyword()]) :: Supervisor.on_start()
  def start_link(connectors), do: Supervisor.start_link(__MODULE__, connectors)

  @impl true
  def init(connectors) do
    children =
      connectors
      |> normalize_connectors()
      |> Enum.with_index()
      |> Enum.map(fn {opts, index} ->
        %{
          id: {:smpp_client, index},
          start: {Client, :start_link, [opts]},
          restart: :transient,
          type: :worker,
          modules: [Client]
        }
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp normalize_connectors([]), do: []
  defp normalize_connectors([head | _] = connectors) when is_tuple(head), do: [connectors]
  defp normalize_connectors(connectors), do: connectors
end
