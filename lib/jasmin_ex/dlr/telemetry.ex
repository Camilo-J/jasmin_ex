defmodule JasminEx.Dlr.Telemetry do
  @moduledoc false

  @allowed [
    :event_id,
    :connector_id,
    :phase,
    :reason_class,
    :attempt,
    :delivery_count,
    :acquired_count,
    :status_code,
    :timestamp
  ]

  @denied [
    :url,
    :query,
    :address,
    :addresses,
    :text,
    :password,
    :credentials,
    :body,
    :callback_url,
    :username,
    :userinfo
  ]

  def emit(event, measurements \\ %{}, metadata)
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute([:jasmin_ex, :dlr | event], measurements, sanitize(metadata))
  end

  defp sanitize(metadata) do
    metadata
    |> Map.take(@allowed)
    |> Map.drop(@denied)
    |> Map.reject(fn {_key, value} -> sensitive?(value) end)
  end

  defp sensitive?(value) when is_binary(value) do
    String.contains?(value, "://") and String.contains?(value, "@")
  end

  defp sensitive?(_value), do: false
end
