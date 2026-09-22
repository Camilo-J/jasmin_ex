defmodule JasminEx.Dlr.Supervisor do
  @moduledoc false
  use Supervisor

  alias JasminEx.Dlr.Config

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name_opts(opts))

  @spec validate_options!(keyword()) :: keyword()
  def validate_options!(opts) when is_list(opts) do
    with %Config{enabled: true} <- Keyword.get(opts, :config),
         {store_module, _store_context} when is_atom(store_module) <- Keyword.get(opts, :store),
         connection when not is_nil(connection) <- Keyword.get(opts, :connection_server),
         {publisher_module, _publisher_context} when is_atom(publisher_module) <-
           Keyword.get(opts, :publisher) do
      opts
    else
      _invalid -> raise ArgumentError, "invalid enabled DLR dependencies"
    end
  end

  @impl true
  def init(opts) do
    validate_options!(opts)

    # WU7-A establishes the supervision and dependency contract. Readiness-owned
    # production workers remain dormant until the WU7-B runtime slice.
    Supervisor.init([], strategy: :one_for_one)
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> []
      name -> [name: name]
    end
  end
end
