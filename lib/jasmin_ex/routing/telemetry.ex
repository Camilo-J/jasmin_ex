defmodule JasminEx.Routing.Telemetry do
  @moduledoc false

  @drop [
    :username,
    :uid,
    :gid,
    :secret,
    :password,
    :salt,
    :digest,
    :source,
    :destination,
    :content,
    :tags,
    :path,
    :address,
    :connector_id,
    :bill_id,
    :fingerprint,
    :balance,
    :balance_minor,
    :quota,
    :submit_quota,
    :rate,
    :rate_minor,
    :precharge_percent,
    :precharge_minor,
    :remainder_minor,
    :captured_minor,
    :reserved_minor,
    :refundable_minor,
    :amount,
    :route_order,
    :route_id,
    :user_id
  ]

  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements \\ %{}, metadata)
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute(
      [:jasmin_ex, :routing | event],
      keep_measurements(event, measurements),
      keep_metadata(event, metadata)
    )
  end

  defp keep_measurements([:billing], measurements),
    do:
      Map.filter(measurements, fn {k, v} -> k in [:count, :bytes] and is_integer(v) and v >= 0 end)

  defp keep_measurements(_event, measurements), do: Map.drop(measurements, @drop)

  defp keep_metadata([:billing], _), do: %{}
  defp keep_metadata(_, metadata), do: Map.drop(metadata, @drop)
end
