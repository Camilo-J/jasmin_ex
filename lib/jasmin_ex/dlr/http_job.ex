defmodule JasminEx.Dlr.HttpJob do
  @moduledoc false

  @common_fields ~w(id level message_status connector)
  @level_two_fields @common_fields ++ ~w(id_smsc sub dlvrd subdate donedate err text)
  @max_url_bytes 8_192
  @max_field_bytes 4_096

  @spec encode(map()) :: {:ok, binary()} | {:error, atom()}
  def encode(job) when is_map(job) do
    with :ok <- validate(job) do
      payload = %{
        "version" => 1,
        "kind" => "http_job",
        "job_id" => job.job_id,
        "event_id" => job.event_id,
        "gateway_id" => job.gateway_id,
        "url" => job.url,
        "method" => job.method,
        "level" => job.level,
        "created_at_ms" => job.created_at_ms,
        "deadline_ms" => job.deadline_ms,
        "fields" => job.fields
      }

      {:ok, payload |> :json.encode() |> IO.iodata_to_binary()}
    end
  rescue
    _error -> {:error, :invalid_job}
  end

  def encode(_job), do: {:error, :invalid_job}

  @spec decode(binary()) :: {:ok, map()} | {:error, atom()}
  def decode(payload) when is_binary(payload) do
    case :json.decode(payload) do
      %{"version" => 1, "kind" => "http_job"} = map -> decode_v1(map)
      %{"version" => 1} -> {:error, :invalid_job}
      %{"version" => _version} -> {:error, :unsupported_version}
      _other -> {:error, :invalid_job}
    end
  rescue
    _error -> {:error, :invalid_job}
  end

  defp decode_v1(map) do
    job = %{
      job_id: map["job_id"],
      event_id: map["event_id"],
      gateway_id: map["gateway_id"],
      url: map["url"],
      method: map["method"],
      level: map["level"],
      created_at_ms: map["created_at_ms"],
      deadline_ms: map["deadline_ms"],
      fields: map["fields"]
    }

    with :ok <- validate(job), do: {:ok, job}
  end

  defp validate(job) do
    with :ok <- binary(job, :job_id),
         :ok <- binary(job, :event_id),
         :ok <- binary(job, :gateway_id),
         :ok <- bounded_binary(job, :url, @max_url_bytes),
         :ok <- method(job),
         :ok <- level(job),
         :ok <- timestamps(job),
         do: fields(job)
  end

  defp binary(job, key) do
    case Map.get(job, key) do
      value when is_binary(value) and value != "" -> :ok
      _other -> {:error, :invalid_job}
    end
  end

  defp bounded_binary(job, key, max) do
    case Map.get(job, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= max -> :ok
      _other -> {:error, :invalid_job}
    end
  end

  defp method(%{method: method}) when method in ["GET", "POST"], do: :ok
  defp method(_job), do: {:error, :invalid_job}
  defp level(%{level: level}) when level in [1, 2], do: :ok
  defp level(_job), do: {:error, :invalid_job}

  defp timestamps(%{created_at_ms: created, deadline_ms: deadline})
       when is_integer(created) and is_integer(deadline) and deadline > created,
       do: :ok

  defp timestamps(_job), do: {:error, :invalid_job}

  defp fields(%{level: level, fields: fields}) when is_map(fields) do
    expected = if level == 1, do: @common_fields, else: @level_two_fields

    if Map.keys(fields) |> Enum.sort() == Enum.sort(expected) and
         Enum.all?(fields, fn {key, value} ->
           is_binary(key) and is_binary(value) and byte_size(value) <= @max_field_bytes
         end),
       do: :ok,
       else: {:error, :invalid_job}
  end

  defp fields(_job), do: {:error, :invalid_job}
end
