defmodule JasminEx.Smpp.Server.Supervisor do
  @moduledoc false
  use Supervisor

  alias JasminEx.Smpp.Server.{BindingManager, Listener, SessionSupervisor}

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name_opts(opts))

  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    router = Keyword.fetch!(opts, :router)
    manager = Keyword.get(opts, :binding_manager, BindingManager)
    sessions = Keyword.get(opts, :session_supervisor, SessionSupervisor)

    Supervisor.init(
      [
        {BindingManager, [name: manager]},
        {SessionSupervisor, [name: sessions]},
        {Listener,
         [
           config: config,
           router: router,
           binding_manager: manager,
           session_supervisor: sessions
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
