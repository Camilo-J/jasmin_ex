defmodule JasminEx.HttpApi.Supervisor do
  @moduledoc false
  use Supervisor

  alias JasminEx.HttpApi.Metrics
  alias JasminEx.HttpApi.Router
  alias JasminEx.Routing.Router, as: RoutingRouter

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name_opts(opts))

  @spec port(Supervisor.supervisor()) :: :inet.port_number()
  def port(supervisor) do
    {{Bandit, _ref}, pid, _type, _modules} =
      Enum.find(Supervisor.which_children(supervisor), fn
        {{Bandit, _ref}, pid, _type, _modules} when is_pid(pid) -> true
        _other -> false
      end)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    port
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    metrics = Keyword.get(opts, :metrics, Metrics)
    router = Keyword.get(opts, :router, RoutingRouter)
    queue = Keyword.get(opts, :queue)

    Supervisor.init(
      [
        %{id: Metrics, start: {Metrics, :start_link, [[name: metrics]]}},
        {Bandit,
         [
           plug: {Router, %{metrics: metrics, router: router, queue: queue}},
           scheme: :http,
           port: config.port,
           ip: config.host
         ]}
      ],
      strategy: :rest_for_one
    )
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name) do
      nil -> []
      name -> [name: name]
    end
  end
end
