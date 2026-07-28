defmodule JasminEx.Smpp.ConnectorSupervisor do
  @moduledoc """
  Supervises isolated SMPP connector subtrees with a one-for-one strategy.

  Each connector owns its client's restart-intensity budget, so one connector
  exhausting its budget does not terminate sibling connectors or this
  supervisor.
  """

  use Supervisor

  alias JasminEx.Smpp.ConnectorSupervisor.Instance

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
          id: {:smpp_connector, index},
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

defmodule JasminEx.Smpp.ConnectorSupervisor.Instance do
  @moduledoc false

  use Supervisor

  alias JasminEx.Smpp.Client

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    child = %{
      id: :smpp_client,
      start: {Client, :start_link, [opts]},
      restart: :transient,
      type: :worker,
      modules: [Client]
    }

    Supervisor.init([child],
      strategy: :one_for_one,
      max_restarts: 3,
      max_seconds: 5
    )
  end
end
