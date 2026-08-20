defmodule JasminEx.Messaging.RabbitMQ.Telemetry do
  @moduledoc false

  @secret_keys [:password, :username, :userinfo, :credentials]
  @confirm %{true => :ok, false => :nack, :timeout => :timeout}

  def emit(event, measurements \\ %{}, metadata)
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute([:jasmin_ex, :messaging | event], measurements, sanitize(metadata))
  end

  def connection_meta(%{host: host, port: port}), do: %{host: host, port: port}
  def confirm_result({:error, :channel_closed}), do: :channel_closed
  def confirm_result({:error, _reason}), do: :error
  def confirm_result(result), do: Map.get(@confirm, result, :error)

  def age_ms(%{enqueued_at: ts}) when is_binary(ts) do
    case DateTime.from_iso8601(ts) do
      {:ok, dt, _} -> max(DateTime.diff(DateTime.utc_now(), dt, :millisecond), 0)
      _ -> 0
    end
  end

  def age_ms(_), do: 0

  def quarantine_meta(config, depth, age_ms, extra \\ %{}) do
    alarms =
      maybe_alarm([], :depth, depth >= config.quarantine_depth_alarm)
      |> maybe_alarm(:age, age_ms >= config.quarantine_age_ms_alarm)

    Map.merge(extra, %{alarm: alarms != [], alarms: Enum.reverse(alarms)})
  end

  defp maybe_alarm(alarms, name, true), do: [name | alarms]
  defp maybe_alarm(alarms, _name, false), do: alarms

  defp sanitize(metadata) do
    metadata
    |> Map.drop(@secret_keys)
    |> Map.reject(fn {_, v} ->
      is_binary(v) and String.contains?(v, "://") and String.contains?(v, "@")
    end)
  end
end
