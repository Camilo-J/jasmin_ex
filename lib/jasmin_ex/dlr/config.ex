defmodule JasminEx.Dlr.Config do
  @moduledoc false

  defstruct enabled: false,
            queue_prefix: "jasmin_ex.dlr",
            lookup_additional_attempts: 2,
            lookup_delay_ms: 10_000,
            http_additional_attempts: 3,
            http_delay_ms: 30_000,
            http_timeout_ms: 30_000,
            dlr_expiry_s: 86_400

  @type t :: %__MODULE__{
          enabled: boolean(),
          queue_prefix: String.t(),
          lookup_additional_attempts: pos_integer(),
          lookup_delay_ms: pos_integer(),
          http_additional_attempts: pos_integer(),
          http_delay_ms: pos_integer(),
          http_timeout_ms: pos_integer(),
          dlr_expiry_s: pos_integer()
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
      dlr_expiry_s: Keyword.get(opts, :dlr_expiry_s, 86_400)
    }

    if valid?(config), do: config, else: {:error, :invalid_dlr_config}
  end

  @spec connector_expiry(t(), pos_integer() | nil) ::
          pos_integer() | {:error, :invalid_dlr_config}
  def connector_expiry(config, expiry \\ nil)

  def connector_expiry(%__MODULE__{dlr_expiry_s: expiry}, nil), do: expiry

  def connector_expiry(%__MODULE__{}, expiry) when is_integer(expiry) and expiry > 0, do: expiry

  def connector_expiry(%__MODULE__{}, _expiry), do: {:error, :invalid_dlr_config}

  defp valid?(%__MODULE__{} = config) do
    is_boolean(config.enabled) and valid_prefix?(config.queue_prefix) and
      positive?(config.lookup_additional_attempts) and positive?(config.lookup_delay_ms) and
      positive?(config.http_additional_attempts) and positive?(config.http_delay_ms) and
      positive?(config.http_timeout_ms) and positive?(config.dlr_expiry_s)
  end

  defp valid_prefix?(prefix) when is_binary(prefix) and prefix != "", do: true
  defp valid_prefix?(_prefix), do: false

  defp positive?(value) when is_integer(value) and value > 0, do: true
  defp positive?(_value), do: false
end
