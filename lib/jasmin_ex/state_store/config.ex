defmodule JasminEx.StateStore.Config do
  @moduledoc false
  @derive {Inspect, except: [:password]}
  @fields ~w(host port username password tls key_namespace connect_timeout_ms command_timeout_ms health_check_timeout_ms backoff_initial_ms backoff_max_ms)a
  defstruct @fields ++ [database: 0]

  def new!(opts) do
    {host, port, tls} = endpoint(opts)
    backoff_initial_ms = positive!(opts, :backoff_initial_ms, 100)
    backoff_max_ms = positive!(opts, :backoff_max_ms, 5_000)

    if backoff_initial_ms > backoff_max_ms,
      do: raise(ArgumentError, "backoff_initial_ms must not exceed backoff_max_ms")

    %__MODULE__{
      host: host,
      port: port,
      tls: tls,
      username: Keyword.get(opts, :username),
      password: Keyword.get(opts, :password),
      key_namespace: {Keyword.get(opts, :prefix, "jasmin_ex"), Keyword.get(opts, :version, "1")},
      connect_timeout_ms: positive!(opts, :connect_timeout_ms, 5_000),
      command_timeout_ms: positive!(opts, :command_timeout_ms, 5_000),
      health_check_timeout_ms: positive!(opts, :health_check_timeout_ms, 5_000),
      backoff_initial_ms: backoff_initial_ms,
      backoff_max_ms: backoff_max_ms
    }
  end

  defp endpoint(opts) do
    case Keyword.get(opts, :endpoint) do
      nil ->
        {Keyword.get(opts, :host, "localhost"), Keyword.get(opts, :port, 6379),
         Keyword.get(opts, :tls, false)}

      endpoint ->
        uri = URI.parse(endpoint)
        if uri.userinfo, do: raise(ArgumentError, "endpoint userinfo is not allowed")

        if uri.scheme not in ["redis", "rediss", "valkey"],
          do: raise(ArgumentError, "endpoint scheme is invalid")

        if uri.path not in [nil, "", "/", "/0"], do: raise(ArgumentError, "database must be 0")
        {uri.host, uri.port || 6379, uri.scheme == "rediss" or Keyword.get(opts, :tls, false)}
    end
  end

  defp positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> raise ArgumentError, "#{key} must be a positive integer"
    end
  end
end
