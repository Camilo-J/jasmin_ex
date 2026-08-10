defmodule JasminEx.Messaging.RabbitMQ.Config do
  @moduledoc false

  @derive {Inspect, except: [:password]}

  @fields [
    :host,
    :port,
    :tls,
    :ssl_options,
    :username,
    :password,
    :virtual_host,
    :queue_prefix,
    :confirm_timeout_ms,
    :max_attempts,
    :default_expiry_ms,
    :quarantine_depth_alarm,
    :quarantine_age_ms_alarm
  ]

  @enforce_keys @fields
  defstruct @fields

  @default_queue_prefix "jasmin.work"
  @default_virtual_host "/"
  @default_confirm_timeout_ms 5_000
  @default_max_attempts 3
  @default_expiry_ms 86_400_000
  @default_quarantine_depth_alarm 1_000
  @default_quarantine_age_ms_alarm 86_400_000
  @default_ssl_options [verify: :verify_peer]
  @queue_prefix_pattern ~r/^[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9]$|^[A-Za-z0-9]$/

  @type t :: %__MODULE__{
          host: String.t(),
          port: pos_integer(),
          tls: boolean(),
          ssl_options: keyword() | :none,
          username: String.t(),
          password: String.t(),
          virtual_host: String.t(),
          queue_prefix: String.t(),
          confirm_timeout_ms: pos_integer(),
          max_attempts: pos_integer(),
          default_expiry_ms: pos_integer(),
          quarantine_depth_alarm: pos_integer(),
          quarantine_age_ms_alarm: pos_integer()
        }

  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    {host, port, tls_from_endpoint} = endpoint(opts)
    tls = tls(opts, tls_from_endpoint)
    ssl_options = ssl_options(opts, tls)
    username = required_string!(opts, :username)
    password = required_string!(opts, :password)
    virtual_host = string_or_default!(opts, :virtual_host, @default_virtual_host)
    queue_prefix = queue_prefix!(opts)

    %__MODULE__{
      host: host,
      port: port,
      tls: tls,
      ssl_options: ssl_options,
      username: username,
      password: password,
      virtual_host: virtual_host,
      queue_prefix: queue_prefix,
      confirm_timeout_ms: positive!(opts, :confirm_timeout_ms, @default_confirm_timeout_ms),
      max_attempts: positive!(opts, :max_attempts, @default_max_attempts),
      default_expiry_ms: positive!(opts, :default_expiry_ms, @default_expiry_ms),
      quarantine_depth_alarm:
        positive!(opts, :quarantine_depth_alarm, @default_quarantine_depth_alarm),
      quarantine_age_ms_alarm:
        positive!(opts, :quarantine_age_ms_alarm, @default_quarantine_age_ms_alarm)
    }
  end

  @spec to_connection_options(t()) :: keyword()
  def to_connection_options(%__MODULE__{} = config) do
    [
      host: String.to_charlist(config.host),
      port: config.port,
      username: config.username,
      password: config.password,
      virtual_host: config.virtual_host,
      ssl_options: config.ssl_options
    ]
  end

  defp endpoint(opts) do
    case Keyword.get(opts, :endpoint) do
      nil ->
        host = required_host!(Keyword.get(opts, :host))
        port = port!(Keyword.get(opts, :port, 5672))
        {host, port, false}

      endpoint when is_binary(endpoint) ->
        parse_endpoint!(endpoint, opts)

      _other ->
        raise ArgumentError, "endpoint must be a string"
    end
  end

  defp parse_endpoint!(endpoint, opts) do
    uri = URI.parse(endpoint)

    if uri.userinfo, do: raise(ArgumentError, "endpoint userinfo is not allowed")

    scheme = uri.scheme

    if scheme not in ["amqp", "amqps"],
      do: raise(ArgumentError, "endpoint scheme is invalid")

    host = required_host!(uri.host)
    default_port = if scheme == "amqps", do: 5671, else: 5672
    port = port!(uri.port || Keyword.get(opts, :port, default_port))
    {host, port, scheme == "amqps"}
  end

  defp required_host!(host) when is_binary(host) and host != "", do: host
  defp required_host!(_host), do: raise(ArgumentError, "endpoint host is required")

  defp port!(port) when is_integer(port) and port > 0 and port <= 65_535, do: port
  defp port!(_port), do: raise(ArgumentError, "endpoint port is invalid")

  defp tls(opts, tls_from_endpoint) do
    case Keyword.get(opts, :tls, tls_from_endpoint) do
      value when is_boolean(value) -> value or tls_from_endpoint
      _other -> raise ArgumentError, "tls must be a boolean"
    end
  end

  defp ssl_options(opts, tls?) do
    opts
    |> Keyword.get(:ssl_options, :default)
    |> normalize_ssl_options(tls?)
  end

  defp normalize_ssl_options(:default, true), do: @default_ssl_options
  defp normalize_ssl_options(:default, false), do: :none
  defp normalize_ssl_options(:none, true), do: @default_ssl_options
  defp normalize_ssl_options(:none, false), do: :none

  defp normalize_ssl_options(ssl_options, true) when is_list(ssl_options) do
    if Keyword.keyword?(ssl_options) do
      ssl_options
    else
      raise ArgumentError, "ssl_options must be a keyword list or :none"
    end
  end

  defp normalize_ssl_options(ssl_options, false) when is_list(ssl_options) do
    raise ArgumentError, "ssl_options require tls"
  end

  defp normalize_ssl_options(_ssl_options, _tls?) do
    raise ArgumentError, "ssl_options must be a keyword list or :none"
  end

  defp queue_prefix!(opts) do
    prefix = Keyword.get(opts, :queue_prefix, @default_queue_prefix)

    if is_binary(prefix) and Regex.match?(@queue_prefix_pattern, prefix) do
      prefix
    else
      raise ArgumentError, "queue_prefix is invalid"
    end
  end

  defp required_string!(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "#{key} is required"
    end
  end

  defp string_or_default!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "#{key} must be a non-empty string"
    end
  end

  defp positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> raise ArgumentError, "#{key} must be a positive integer"
    end
  end
end
