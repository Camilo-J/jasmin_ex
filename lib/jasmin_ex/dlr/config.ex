defmodule JasminEx.Dlr.Config do
  @moduledoc false

  @default_window_s 3_600

  defstruct enabled: false,
            queue_prefix: "jasmin_ex.dlr",
            lookup_additional_attempts: 2,
            lookup_delay_ms: 10_000,
            http_additional_attempts: 3,
            http_delay_ms: 30_000,
            http_timeout_ms: 30_000,
            dlr_deadline_s: @default_window_s,
            dlr_closed_retention_s: @default_window_s,
            connector_windows: %{}

  @type windows :: %{deadline_s: pos_integer(), closed_retention_s: pos_integer()}

  @type t :: %__MODULE__{
          enabled: boolean(),
          queue_prefix: String.t(),
          lookup_additional_attempts: pos_integer(),
          lookup_delay_ms: pos_integer(),
          http_additional_attempts: pos_integer(),
          http_delay_ms: pos_integer(),
          http_timeout_ms: pos_integer(),
          dlr_deadline_s: pos_integer(),
          dlr_closed_retention_s: pos_integer(),
          connector_windows: %{optional(String.t()) => map()}
        }

  @spec new(keyword()) :: t() | {:error, :invalid_dlr_config}
  def new(opts \\ []) when is_list(opts) do
    config = %__MODULE__{
      enabled: Keyword.get(opts, :enabled, false),
      queue_prefix: Keyword.get(opts, :queue_prefix, "jasmin_ex.dlr"),
      lookup_additional_attempts: Keyword.get(opts, :lookup_additional_attempts, 2),
      lookup_delay_ms: Keyword.get(opts, :lookup_delay_ms, 10_000),
      http_additional_attempts: Keyword.get(opts, :http_additional_attempts, 3),
      http_delay_ms: Keyword.get(opts, :http_delay_ms, 30_000),
      http_timeout_ms: Keyword.get(opts, :http_timeout_ms, 30_000),
      dlr_deadline_s: Keyword.get(opts, :dlr_deadline_s, @default_window_s),
      dlr_closed_retention_s: Keyword.get(opts, :dlr_closed_retention_s, @default_window_s)
    }

    if valid?(config), do: config, else: {:error, :invalid_dlr_config}
  end

  @doc """
  Records explicit per-connector `dlr_deadline`, `dlr_closed_retention`, and
  `dlr_correlation_group` values. Connectors without an override use the
  global values and their own connector id as correlation group.
  """
  @spec with_connectors(t(), keyword() | [keyword() | map()]) ::
          t() | {:error, :invalid_dlr_config}
  def with_connectors(%__MODULE__{} = config, connectors) do
    connectors
    |> connector_list()
    |> Enum.reduce_while(config, &put_connector/2)
  end

  @spec windows(t(), String.t()) :: windows()
  def windows(%__MODULE__{} = config, connector_id) do
    overrides = Map.get(config.connector_windows, connector_id, %{})

    %{
      deadline_s: Map.get(overrides, :deadline_s, config.dlr_deadline_s),
      closed_retention_s: Map.get(overrides, :closed_retention_s, config.dlr_closed_retention_s)
    }
  end

  @doc """
  Correlation group of `connector_id`: its explicit `dlr_correlation_group`,
  or its own connector id.
  """
  @spec correlation_group(t(), String.t()) :: String.t()
  def correlation_group(%__MODULE__{} = config, connector_id) do
    config.connector_windows
    |> Map.get(connector_id, %{})
    |> Map.get(:correlation_group, connector_id)
  end

  @doc """
  Correlation lifetime of a message submitted by `connector_id`: the delivery
  decision deadline plus the post-closure retention window.
  """
  @spec lifetime_s(t(), String.t()) :: pos_integer()
  def lifetime_s(%__MODULE__{} = config, connector_id) do
    %{deadline_s: deadline, closed_retention_s: retention} = windows(config, connector_id)
    deadline + retention
  end

  defp connector_list(connectors) do
    if Keyword.keyword?(connectors), do: [connectors], else: connectors
  end

  defp put_connector(connector, config) do
    case connector_overrides(connector) do
      {:ok, nil, _overrides} ->
        {:cont, config}

      {:ok, connector_id, overrides} ->
        {:cont, put_in(config.connector_windows[connector_id], overrides)}

      :error ->
        {:halt, {:error, :invalid_dlr_config}}
    end
  end

  defp connector_overrides(connector) do
    opts = Map.new(connector)

    overrides =
      for {key, field} <- [dlr_deadline: :deadline_s, dlr_closed_retention: :closed_retention_s],
          Map.has_key?(opts, key),
          into: %{},
          do: {field, Map.fetch!(opts, key)}

    with true <- Enum.all?(Map.values(overrides), &positive?/1),
         {:ok, overrides} <- put_group(overrides, opts) do
      {:ok, Map.get(opts, :connector_id), overrides}
    else
      _invalid -> :error
    end
  end

  defp put_group(overrides, %{dlr_correlation_group: group})
       when is_binary(group) and group != "",
       do: {:ok, Map.put(overrides, :correlation_group, group)}

  defp put_group(_overrides, %{dlr_correlation_group: _invalid}), do: :error
  defp put_group(overrides, _opts), do: {:ok, overrides}

  defp valid?(%__MODULE__{} = config) do
    is_boolean(config.enabled) and valid_prefix?(config.queue_prefix) and
      positive?(config.lookup_additional_attempts) and positive?(config.lookup_delay_ms) and
      positive?(config.http_additional_attempts) and positive?(config.http_delay_ms) and
      positive?(config.http_timeout_ms) and positive?(config.dlr_deadline_s) and
      positive?(config.dlr_closed_retention_s)
  end

  defp valid_prefix?(prefix) when is_binary(prefix) and prefix != "", do: true
  defp valid_prefix?(_prefix), do: false

  defp positive?(value) when is_integer(value) and value > 0, do: true
  defp positive?(_value), do: false
end
