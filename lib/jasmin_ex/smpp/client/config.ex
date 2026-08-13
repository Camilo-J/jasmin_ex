defmodule JasminEx.Smpp.Client.Config do
  @moduledoc false

  alias JasminEx.Smpp.Client.ReconnectPolicy

  @default_heartbeat_ms 30_000
  @default_response_timeout_ms 5_000

  @enforce_keys [
    :connector_id,
    :host,
    :port,
    :system_id,
    :password,
    :system_type,
    :bind_as,
    :heartbeat_ms,
    :response_timeout_ms,
    :unbind_drain_timeout_ms,
    :reconnect,
    :deliver_handler
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          connector_id: String.t(),
          host: term(),
          port: term(),
          system_id: term(),
          password: term(),
          system_type: term(),
          bind_as: term(),
          heartbeat_ms: term(),
          response_timeout_ms: term(),
          unbind_drain_timeout_ms: non_neg_integer(),
          reconnect: ReconnectPolicy.t(),
          deliver_handler: {module() | nil, term()}
        }

  @spec new!(keyword()) :: t()
  def new!(opts) do
    response_timeout_ms = Keyword.get(opts, :response_timeout_ms, @default_response_timeout_ms)

    unbind_drain_timeout_ms =
      opts
      |> Keyword.get(:unbind_drain_timeout_ms, response_timeout_ms)
      |> validate_unbind_drain_timeout!()

    %__MODULE__{
      connector_id: connector_id!(opts),
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      system_id: Keyword.fetch!(opts, :system_id),
      password: Keyword.fetch!(opts, :password),
      system_type: Keyword.fetch!(opts, :system_type),
      bind_as: Keyword.fetch!(opts, :bind_as),
      heartbeat_ms: Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms),
      response_timeout_ms: response_timeout_ms,
      unbind_drain_timeout_ms: unbind_drain_timeout_ms,
      reconnect: ReconnectPolicy.new(opts),
      deliver_handler: normalize_deliver_handler(Keyword.get(opts, :deliver_handler))
    }
  end

  defp connector_id!(opts) do
    case Keyword.fetch!(opts, :connector_id) do
      connector_id when is_binary(connector_id) and connector_id != "" ->
        connector_id

      connector_id ->
        raise ArgumentError,
              ":connector_id must be a non-empty binary, got: #{inspect(connector_id)}"
    end
  end

  defp validate_unbind_drain_timeout!(timeout) when is_integer(timeout) and timeout >= 0,
    do: timeout

  defp validate_unbind_drain_timeout!(timeout) do
    raise ArgumentError,
          ":unbind_drain_timeout_ms must be a non-negative integer, got: #{inspect(timeout)}"
  end

  defp normalize_deliver_handler({handler, context}) when is_atom(handler), do: {handler, context}
  defp normalize_deliver_handler(nil), do: {nil, nil}
end
