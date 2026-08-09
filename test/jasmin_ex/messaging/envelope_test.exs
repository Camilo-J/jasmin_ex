defmodule JasminEx.Messaging.EnvelopeTest do
  use ExUnit.Case, async: true
  alias JasminEx.Messaging.Envelope

  test "rejects unsupported versions without creating atoms from JSON values" do
    atom_name = "untrusted_connector_#{System.unique_integer([:positive])}"
    payload = ~s({"version":2,"connector_id":"#{atom_name}"})
    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
    assert Envelope.decode(payload) == {:error, :unsupported_version}
    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
  end

  test "rejects unknown version one keys without creating atoms" do
    unknown_key = "untrusted_key_#{System.unique_integer([:positive])}"
    payload = ~s({"version":1,"#{unknown_key}":"value"})

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end
    assert Envelope.decode(payload) == {:error, :invalid_envelope}
    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end
  end

  test "encodes and decodes a validated version one submission envelope" do
    attributes = %{
      gateway_id: "gateway-1",
      connector_id: "connector-a",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2026-08-02T15:00:00Z",
      submit_sm: %{
        source_addr: "+12025550100",
        destination_addr: "+12025550101",
        short_message: "hello"
      }
    }

    assert {:ok, envelope} = Envelope.new(attributes)
    assert {:ok, encoded} = Envelope.encode(envelope)
    assert {:ok, decoded} = Envelope.decode(encoded)
    assert decoded == envelope
  end

  test "rejects invalid attempt bounds and submit payloads" do
    assert Envelope.new(%{
             gateway_id: "gateway-1",
             connector_id: "connector-a",
             attempt: 2,
             max_attempts: 1
           }) ==
             {:error, :invalid_envelope}

    assert Envelope.new(%{
             gateway_id: "gateway-1",
             connector_id: "connector-a",
             attempt: 1,
             max_attempts: 1,
             enqueued_at: "2026-08-01T15:00:00Z",
             expires_at: "2026-08-02T15:00:00Z",
             submit_sm: %{
               source_addr: "+12025550100",
               destination_addr: "+12025550101",
               short_message: :invalid
             }
           }) == {:error, :invalid_envelope}
  end

  test "rejects caller maps with unknown keys without raising" do
    attributes = %{
      gateway_id: "gateway-1",
      connector_id: "connector-a",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2026-08-02T15:00:00Z",
      submit_sm: %{
        source_addr: "+12025550100",
        destination_addr: "+12025550101",
        short_message: "hello"
      },
      unexpected: "drop-me"
    }

    assert Envelope.new(attributes) == {:error, :invalid_envelope}
  end

  test "decode still projects only supported fields from JSON with extra keys" do
    payload =
      ~s({"version":1,"gateway_id":"gateway-1","connector_id":"connector-a","attempt":1,"max_attempts":3,"enqueued_at":"2026-08-01T15:00:00Z","expires_at":"2026-08-02T15:00:00Z","submit_sm":{"source_addr":"+12025550100","destination_addr":"+12025550101","short_message":"hello"},"extra":"ignored"})

    assert {:ok, envelope} = Envelope.decode(payload)
    assert envelope.gateway_id == "gateway-1"
    assert envelope.connector_id == "connector-a"
    refute Map.has_key?(Map.from_struct(envelope), :extra)
  end
end
