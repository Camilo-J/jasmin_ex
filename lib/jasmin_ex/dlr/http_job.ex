defmodule JasminEx.Dlr.HttpJob do
  @moduledoc false

  @version 2
  @max_url_bytes 8_192
  @max_payload_bytes 65_536
  @statuses ~w(ACCEPTED DELIVERED FAILED UNRESOLVED)

  @spec encode(map()) :: {:ok, binary()} | {:error, atom()}
  def encode(job) when is_map(job) do
    with :ok <- validate(job) do
      payload = %{
        "version" => @version,
        "kind" => "http_job",
        "job_id" => job.job_id,
        "event_id" => job.event_id,
        "gateway_id" => job.gateway_id,
        "url" => job.url,
        "level" => job.level,
        "created_at_ms" => job.created_at_ms,
        "deadline_ms" => job.deadline_ms,
        "payload" => job.payload
      }

      {:ok, json(payload)}
    end
  rescue
    _error -> {:error, :invalid_job}
  end

  def encode(_job), do: {:error, :invalid_job}

  @spec decode(binary()) :: {:ok, map()} | {:error, atom()}
  def decode(payload) when is_binary(payload) do
    case :json.decode(payload) do
      %{"version" => @version, "kind" => "http_job"} = map -> decode_current(map)
      %{"version" => @version} -> {:error, :invalid_job}
      %{"version" => _version} -> {:error, :unsupported_version}
      _other -> {:error, :invalid_job}
    end
  rescue
    _error -> {:error, :invalid_job}
  end

  @doc "The callback request body: the unified JSON payload."
  @spec body(map()) :: binary()
  def body(%{payload: payload}), do: json(payload)

  defp decode_current(map) do
    job = %{
      job_id: map["job_id"],
      event_id: map["event_id"],
      gateway_id: map["gateway_id"],
      url: map["url"],
      level: map["level"],
      created_at_ms: map["created_at_ms"],
      deadline_ms: map["deadline_ms"],
      payload: map["payload"]
    }

    with :ok <- validate(job), do: {:ok, job}
  end

  defp validate(job) do
    with :ok <- binary(job, :job_id),
         :ok <- binary(job, :event_id),
         :ok <- binary(job, :gateway_id),
         :ok <- bounded_binary(job, :url, @max_url_bytes),
         :ok <- level(job),
         :ok <- timestamps(job),
         do: payload(job)
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

  defp level(%{level: level}) when level in [1, 2], do: :ok
  defp level(_job), do: {:error, :invalid_job}

  defp timestamps(%{created_at_ms: created, deadline_ms: deadline})
       when is_integer(created) and is_integer(deadline) and deadline > created,
       do: :ok

  defp timestamps(_job), do: {:error, :invalid_job}

  defp payload(%{gateway_id: id, level: level, payload: payload}) do
    with %{
           "schema_version" => 1,
           "id" => ^id,
           "level" => ^level,
           "status" => status,
           "status_origin" => "gateway",
           "total_segments" => total,
           "segments" => [_ | _] = segments
         }
         when status in @statuses and is_integer(total) and total > 0 <- payload,
         true <- Enum.all?(segments, &segment?/1),
         true <- byte_size(json(payload)) <= @max_payload_bytes do
      :ok
    else
      _invalid -> {:error, :invalid_job}
    end
  end

  defp payload(_job), do: {:error, :invalid_job}

  defp segment?(%{"index" => index}) when is_integer(index) and index > 0, do: true
  defp segment?(_segment), do: false

  defp json(term), do: term |> :json.encode() |> IO.iodata_to_binary()
end
