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
    :connector_id
  ]

  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements \\ %{}, metadata)
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute([:jasmin_ex, :routing | event], measurements, Map.drop(metadata, @drop))
  end
end
