defmodule JasminEx.Dlr.Payload do
  @moduledoc """
  Builds the unified JSON DLR callback payload (`schema_version` 1).

  The root `status` is the gateway conclusion for the stage; provider
  evidence is kept per segment in `segments`, with missing evidence omitted.
  """

  @schema_version 1
  @failure_statuses ~w(UNDELIV REJECTD EXPIRED DELETED)
  @receipt_fields [
    {"err", :err},
    {"sub", :sub},
    {"dlvrd", :dlvrd},
    {"subdate", :subdate},
    {"donedate", :donedate},
    {"text", :text}
  ]

  @type classification :: :delivered | :failed | :intermediate

  @doc """
  Classifies a provider receipt status: `DELIVRD` is delivered, `UNDELIV`,
  `REJECTD`, `EXPIRED`, and `DELETED` are definitive failures, and anything
  else is intermediate.
  """
  @spec classify(String.t()) :: classification()
  def classify("DELIVRD"), do: :delivered
  def classify(status) when status in @failure_statuses, do: :failed
  def classify(_status), do: :intermediate

  @doc "Submission acceptance (level 1) payload for a single-segment message."
  @spec acceptance(map(), map()) :: map()
  def acceptance(request, event) do
    status = if event.status == "ESME_ROK", do: "ACCEPTED", else: "FAILED"

    segment =
      %{"index" => 1, "message_status" => event.status}
      |> put_evidence("id_smsc", event.raw_smsc_id)

    root(request, 1, status, [segment])
  end

  @doc "Delivery (level 2) payload for a single-segment message."
  @spec delivery(map(), map(), String.t()) :: map()
  def delivery(request, event, status) do
    segment =
      Enum.reduce(
        @receipt_fields,
        %{"index" => 1, "message_status" => event.status},
        fn {key, field}, acc -> put_evidence(acc, key, Map.get(event, field)) end
      )
      |> put_evidence("id_smsc", event.raw_smsc_id)
      |> Map.put("received_at", received_at(event.observed_at_ms))

    root(request, 2, status, [segment])
  end

  defp root(request, level, status, segments) do
    %{
      "schema_version" => @schema_version,
      "id" => request.gateway_id,
      "level" => level,
      "status" => status,
      "status_origin" => "gateway",
      "connector" => request.connector_id,
      "total_segments" => length(segments),
      "segments" => segments
    }
  end

  # `ND` and empty values are missing evidence, never fabricated values.
  defp put_evidence(segment, _key, value) when value in [nil, "", "ND"], do: segment
  defp put_evidence(segment, key, value) when is_binary(value), do: Map.put(segment, key, value)
  defp put_evidence(segment, _key, _value), do: segment

  defp received_at(observed_at_ms) do
    observed_at_ms
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_iso8601()
  end
end
