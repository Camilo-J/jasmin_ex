defmodule JasminEx.StateStore.Redix do
  @moduledoc false

  alias JasminEx.StateStore.{Config, Key}

  def context(%Config{} = config) do
    %{
      connection: JasminEx.StateStore.Connection,
      command_timeout_ms: config.command_timeout_ms,
      key_namespace: config.key_namespace
    }
  end

  def fetch(context, key) do
    with physical_key when is_binary(physical_key) <- Key.encode(context.key_namespace, key) do
      case command(context, ["GET", physical_key]) do
        {:ok, value} when is_binary(value) -> {:ok, value}
        {:ok, nil} -> :missing
        {:ok, _unexpected} -> {:error, {:store, :protocol}}
        {:error, reason} -> {:error, confirmed_failure(reason)}
      end
    end
  end

  def put(context, key, value, ttl_ms) do
    with physical_key when is_binary(physical_key) <- Key.encode(context.key_namespace, key) do
      case command(context, ["SET", physical_key, value, "PX", Integer.to_string(ttl_ms)]) do
        {:ok, "OK"} -> :ok
        {:ok, _unexpected} -> {:error, {:store, :protocol}}
        {:error, reason} -> mutation_failure(reason)
      end
    end
  end

  def delete(context, key) do
    with physical_key when is_binary(physical_key) <- Key.encode(context.key_namespace, key) do
      case command(context, ["DEL", physical_key]) do
        {:ok, 1} -> :deleted
        {:ok, 0} -> :missing
        {:ok, _unexpected} -> {:error, {:store, :protocol}}
        {:error, reason} -> mutation_failure(reason)
      end
    end
  end

  defp command(
         %{command: command, connection: connection, command_timeout_ms: timeout},
         redis_command
       ),
       do: command.(connection, redis_command, timeout)

  defp command(%{connection: connection, command_timeout_ms: timeout}, redis_command),
    do: Redix.command(connection, redis_command, timeout: timeout)

  defp confirmed_failure(%Redix.Error{}), do: {:store, :rejected}
  defp confirmed_failure(reason) when reason in [:closed, :noproc], do: {:store, :unavailable}

  defp confirmed_failure(%Redix.ConnectionError{reason: :closed}), do: {:store, :unavailable}
  defp confirmed_failure(_reason), do: {:store, :protocol}

  defp mutation_failure(%Redix.Error{}), do: {:error, {:store, :rejected}}

  defp mutation_failure(reason) when reason in [:closed, :noproc],
    do: {:error, {:store, :unavailable}}

  defp mutation_failure(%Redix.ConnectionError{reason: :closed}),
    do: {:error, {:store, :unavailable}}

  defp mutation_failure(%Redix.ConnectionError{reason: reason})
       when reason in [:timeout, :disconnected, :health_check_timeout],
       do: {:ambiguous, {:store, reason}}

  defp mutation_failure(reason) when reason in [:timeout, :disconnected, :health_check_timeout],
    do: {:ambiguous, {:store, reason}}

  defp mutation_failure(_reason), do: {:ambiguous, {:store, :disconnected}}
end
