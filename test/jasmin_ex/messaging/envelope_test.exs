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

  test "round-trips basic data_coding in queued submit_sm" do
    for data_coding <- [0, 1, 2, 3, 8] do
      attributes = valid_attributes(data_coding: data_coding)
      assert {:ok, envelope} = Envelope.new(attributes)
      assert envelope.submit_sm.data_coding == data_coding
      assert {:ok, encoded} = Envelope.encode(envelope)
      assert {:ok, decoded} = Envelope.decode(encoded)
      assert decoded.submit_sm.data_coding == data_coding
      assert decoded == envelope
    end
  end

  test "preserves registered_delivery 1 across encode, decode, and retry rebuild" do
    attributes = valid_attributes(data_coding: 0, registered_delivery: 1)
    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.registered_delivery == 1
    assert {:ok, encoded} = Envelope.encode(envelope)
    assert {:ok, decoded} = Envelope.decode(encoded)
    assert decoded.submit_sm.registered_delivery == 1
    assert decoded == envelope

    retried = %{
      gateway_id: envelope.gateway_id,
      connector_id: envelope.connector_id,
      attempt: 2,
      max_attempts: envelope.max_attempts,
      enqueued_at: envelope.enqueued_at,
      expires_at: envelope.expires_at,
      submit_sm: envelope.submit_sm
    }

    assert {:ok, next} = Envelope.new(retried)
    assert next.submit_sm.registered_delivery == 1
  end

  test "omitted and old messages default registered_delivery to 0" do
    attributes = valid_attributes(data_coding: 0)
    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.registered_delivery == 0
    assert {:ok, encoded} = Envelope.encode(envelope)
    assert {:ok, decoded} = Envelope.decode(encoded)
    assert decoded.submit_sm.registered_delivery == 0

    payload =
      ~s({"version":1,"gateway_id":"gateway-1","connector_id":"connector-a","attempt":1,"max_attempts":3,"enqueued_at":"2026-08-01T15:00:00Z","expires_at":"2026-08-02T15:00:00Z","submit_sm":{"source_addr":"+12025550100","destination_addr":"+12025550101","short_message":"hello","data_coding":0}})

    assert {:ok, legacy} = Envelope.decode(payload)
    assert legacy.submit_sm.registered_delivery == 0
  end

  test "callback URLs are not stored on the envelope" do
    attributes =
      valid_attributes(data_coding: 0)
      |> Map.put(:dlr_url, "http://example.com/dlr")

    assert Envelope.new(attributes) == {:error, :invalid_envelope}

    attributes =
      valid_attributes(data_coding: 0)
      |> put_in([:submit_sm, :callback_url], "http://example.com/dlr")

    assert {:ok, envelope} = Envelope.new(attributes)
    refute Map.has_key?(envelope.submit_sm, :callback_url)
    refute Map.has_key?(envelope.submit_sm, :dlr_url)
  end

  test "rejects data_coding outside 0, 1, 2, 3, 8" do
    for data_coding <- [4, 7, 99, -1] do
      assert Envelope.new(valid_attributes(data_coding: data_coding)) ==
               {:error, :invalid_envelope}
    end
  end

  defp valid_attributes(overrides) do
    data_coding = Keyword.fetch!(overrides, :data_coding)

    submit_sm = %{
      source_addr: "+12025550100",
      destination_addr: "+12025550101",
      short_message: "hello",
      data_coding: data_coding
    }

    submit_sm =
      case Keyword.get(overrides, :registered_delivery) do
        nil -> submit_sm
        value -> Map.put(submit_sm, :registered_delivery, value)
      end

    %{
      gateway_id: "gateway-1",
      connector_id: "connector-a",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2026-08-02T15:00:00Z",
      submit_sm: submit_sm
    }
  end
end
