defmodule JasminEx.Smpp.ConnectorSupervisor do
  @moduledoc """
  Supervises transient SMPP client sessions with a one-for-one strategy.

  An abnormal child exit restarts only that child, while all children share
  this supervisor's restart-intensity budget.
  """

  use Supervisor

  alias JasminEx.Smpp.Client

  @invalid_connectors_message "invalid SMPP connector configuration: expected [], one non-empty keyword connector configuration, or a list of non-empty keyword connector configurations"

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

    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: 3,
      max_seconds: 5
    )
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
