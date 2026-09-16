defmodule JasminEx.Dlr.TelemetryTest do
  use ExUnit.Case, async: false

  alias JasminEx.Dlr.Telemetry

  setup do
    handler_id = "dlr-telemetry-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:jasmin_ex, :dlr, :settlement],
          [:jasmin_ex, :dlr, :terminal]
        ],
        &__MODULE__.handle_event/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "emits allowlisted settlement metadata" do
    Telemetry.emit([:settlement], %{count: 1}, %{
      event_id: "e1",
      connector_id: "c1",
      phase: :lookup,
      reason_class: :ok,
      attempt: 1,
      delivery_count: 0,
      acquired_count: 1,
      status_code: 200,
      timestamp: 1_700_000_000
    })

    assert_receive {:telemetry, [:jasmin_ex, :dlr, :settlement], %{count: 1}, meta}
    assert meta.event_id == "e1"
    assert meta.connector_id == "c1"
    assert meta.phase == :lookup
    assert meta.reason_class == :ok
    assert meta.attempt == 1
    assert meta.delivery_count == 0
    assert meta.acquired_count == 1
    assert meta.status_code == 200
    assert meta.timestamp == 1_700_000_000
  end

  test "omits URL, query, addresses, text, credentials, and response bodies" do
    Telemetry.emit([:terminal], %{}, %{
      event_id: "e2",
      connector_id: "c1",
      phase: :http,
      reason_class: :exhausted,
      url: "https://example.com/dlr?token=secret",
      query: "id=G1",
      address: "10.0.0.1",
      text: "hello world",
      password: "s3cret",
      credentials: "user:pass",
      body: "ACK/Jasmin",
      callback_url: "http://127.0.0.1/dlr"
    })

    assert_receive {:telemetry, [:jasmin_ex, :dlr, :terminal], %{}, meta}
    assert meta.event_id == "e2"
    refute Map.has_key?(meta, :url)
    refute Map.has_key?(meta, :query)
    refute Map.has_key?(meta, :address)
    refute Map.has_key?(meta, :text)
    refute Map.has_key?(meta, :password)
    refute Map.has_key?(meta, :credentials)
    refute Map.has_key?(meta, :body)
    refute Map.has_key?(meta, :callback_url)
    refute Enum.any?(Map.values(meta), &sensitive?/1)
  end

  def handle_event(event, measurements, metadata, parent) do
    send(parent, {:telemetry, event, measurements, metadata})
  end

  defp sensitive?(value) when is_binary(value) do
    String.contains?(value, "://") or String.contains?(value, "s3cret") or
      String.contains?(value, "ACK/Jasmin")
  end

  defp sensitive?(_value), do: false
end
