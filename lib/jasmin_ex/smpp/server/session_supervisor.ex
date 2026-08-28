defmodule JasminEx.Smpp.Server.SessionSupervisor do
  @moduledoc false
  use DynamicSupervisor

  alias JasminEx.Smpp.Server.Session

  def start_link(opts),
    do: DynamicSupervisor.start_link(__MODULE__, :ok, name_opts(opts))

  def init(:ok), do: DynamicSupervisor.init(strategy: :one_for_one)

  def start_session(sup, opts) do
    DynamicSupervisor.start_child(sup, %{
      id: Session,
      start: {Session, :start_link, [opts]},
      restart: :temporary
    })
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name) do
      nil -> []
      name -> [name: name]
    end
  end
end
