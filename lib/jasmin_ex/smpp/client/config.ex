defmodule JasminEx.Smpp.Client.Config do
  @moduledoc false

  alias JasminEx.Smpp.Client.ReconnectPolicy

  @default_heartbeat_ms 30_000
  @default_response_timeout_ms 5_000
  @default_dlr_window_s 3_600

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
    :deliver_handler,
    :lifecycle_notify,
    :dlr_deadline,
    :dlr_closed_retention,
    :dlr_correlation_group
  ]
  defstruct @enforce_keys ++ [dlr_publisher: nil]

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
          deliver_handler: {module() | nil, term()},
          lifecycle_notify: pid() | atom() | nil,
          dlr_deadline: pos_integer(),
          dlr_closed_retention: pos_integer(),
          dlr_correlation_group: String.t(),
          dlr_publisher: {module(), term()} | nil
        }

  @spec new!(keyword()) :: t()
  def new!(opts) do
    response_timeout_ms = Keyword.get(opts, :response_timeout_ms, @default_response_timeout_ms)

    unbind_drain_timeout_ms =
      opts
      |> Keyword.get(:unbind_drain_timeout_ms, response_timeout_ms)
      |> validate_unbind_drain_timeout!()

    connector_id = connector_id!(opts)

    %__MODULE__{
      connector_id: connector_id,
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
      deliver_handler: normalize_deliver_handler(Keyword.get(opts, :deliver_handler)),
      lifecycle_notify: lifecycle_notify!(Keyword.get(opts, :lifecycle_notify)),
      dlr_deadline: dlr_window!(opts, :dlr_deadline),
      dlr_closed_retention: dlr_window!(opts, :dlr_closed_retention),
      dlr_correlation_group:
        correlation_group!(Keyword.get(opts, :dlr_correlation_group, connector_id)),
      dlr_publisher: dlr_publisher!(Keyword.get(opts, :dlr_publisher))
    }
  end

  @doc """
  Correlation lifetime of messages submitted by this connector: the delivery
  decision deadline plus the post-closure retention window, in seconds.
  """
  @spec dlr_lifetime_s(t()) :: pos_integer()
  def dlr_lifetime_s(%__MODULE__{dlr_deadline: deadline, dlr_closed_retention: retention}),
    do: deadline + retention

  defp lifecycle_notify!(notify) when is_pid(notify) or is_atom(notify), do: notify

  defp lifecycle_notify!(notify) do
    raise ArgumentError, ":lifecycle_notify must be a pid, atom, or nil, got: #{inspect(notify)}"
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

  defp dlr_window!(opts, key) do
    case Keyword.get(opts, key, @default_dlr_window_s) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError, "#{inspect(key)} must be a positive integer, got: #{inspect(value)}"
    end
  end

  defp correlation_group!(group) when is_binary(group) and group != "", do: group

  defp correlation_group!(group) do
    raise ArgumentError,
          ":dlr_correlation_group must be a non-empty binary, got: #{inspect(group)}"
  end

  defp dlr_publisher!(nil), do: nil
  defp dlr_publisher!({module, context}) when is_atom(module), do: {module, context}

  defp dlr_publisher!(value) do
    raise ArgumentError,
          ":dlr_publisher must be nil or {module, context}, got: #{inspect(value)}"
  end
end
