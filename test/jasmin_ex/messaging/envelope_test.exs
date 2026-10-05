defmodule JasminEx.Messaging.EnvelopeTest do
  use ExUnit.Case, async: true
  alias JasminEx.Messaging.Envelope
  alias JasminEx.Smpp.PDU.{Body, Tlv}

  test "rejects unsupported versions without creating atoms from JSON values" do
    atom_name = "untrusted_connector_#{System.unique_integer([:positive])}"
    payload = ~s({"version":99,"connector_id":"#{atom_name}"})
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

  test "encodes and decodes a validated submission envelope" do
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

  test "encode writes integer version 2 and canonical short_message_base64" do
    assert {:ok, envelope} = Envelope.new(valid_attributes(data_coding: 0))
    assert {:ok, encoded} = Envelope.encode(envelope)
    wire = :json.decode(encoded)

    assert wire["version"] === 2
    refute Map.has_key?(wire["submit_sm"], "short_message")
    assert wire["submit_sm"]["short_message_base64"] == Base.encode64("hello")
    assert wire["submit_sm"]["short_message_base64"] == "aGVsbG8="
  end

  test "round-trips exact binary payloads including empty" do
    for {label, message} <- binary_payloads() do
      attributes = valid_attributes(data_coding: 0, short_message: message)
      assert {:ok, envelope} = Envelope.new(attributes), label
      assert envelope.submit_sm.short_message === message
      refute Map.has_key?(envelope.submit_sm, :short_message_base64)
      assert {:ok, encoded} = Envelope.encode(envelope), label
      wire = :json.decode(encoded)
      assert wire["version"] === 2, label
      assert wire["submit_sm"]["short_message_base64"] == Base.encode64(message)
      assert {:ok, decoded} = Envelope.decode(encoded), label
      assert decoded.submit_sm.short_message === message
      assert decoded.gateway_id == envelope.gateway_id
      assert decoded.connector_id == envelope.connector_id
      assert decoded.attempt == envelope.attempt
      assert decoded.max_attempts == envelope.max_attempts
      assert decoded.enqueued_at == envelope.enqueued_at
      assert decoded.expires_at == envelope.expires_at
      assert decoded == envelope
    end
  end

  test "preserves coding and delivery flags with binary short_message" do
    message = <<0, 255, 1, 27, 20>>

    for data_coding <- [0, 1, 2, 3, 8], registered_delivery <- [0, 1] do
      attributes =
        valid_attributes(
          data_coding: data_coding,
          registered_delivery: registered_delivery,
          short_message: message
        )

      assert {:ok, envelope} = Envelope.new(attributes)
      assert {:ok, encoded} = Envelope.encode(envelope)
      assert {:ok, decoded} = Envelope.decode(encoded)
      assert decoded.submit_sm.short_message === message
      assert decoded.submit_sm.data_coding == data_coding
      assert decoded.submit_sm.registered_delivery == registered_delivery
      assert decoded == envelope
    end
  end

  test "decodes literal v1 fixtures without Base64 or transcoding" do
    non_ascii = v1_fixture(%{"short_message" => "café", "data_coding" => 3})
    looking = v1_fixture(%{"short_message" => "aGVsbG8="})

    assert {:ok, accented} = Envelope.decode(non_ascii)
    assert accented.submit_sm.short_message === "café"

    assert {:ok, literal} = Envelope.decode(looking)
    assert literal.submit_sm.short_message === "aGVsbG8="
    refute literal.submit_sm.short_message === "hello"
    assert literal.submit_sm.data_coding == 0
    assert literal.submit_sm.registered_delivery == 0
  end

  test "v1 extra short_message_base64 stays literal and base64-only v1 is invalid" do
    both =
      v1_fixture(%{
        "short_message" => "aGVsbG8=",
        "short_message_base64" => Base.encode64(<<0, 255>>)
      })

    assert {:ok, envelope} = Envelope.decode(both)
    assert envelope.submit_sm.short_message === "aGVsbG8="
    refute envelope.submit_sm.short_message === "hello"
    refute envelope.submit_sm.short_message === <<0, 255>>
    refute Map.has_key?(envelope.submit_sm, :short_message_base64)

    only_base64 =
      %{
        "version" => 1,
        "gateway_id" => "gateway-1",
        "connector_id" => "connector-a",
        "attempt" => 1,
        "max_attempts" => 3,
        "enqueued_at" => "2026-08-01T15:00:00Z",
        "expires_at" => "2026-08-02T15:00:00Z",
        "submit_sm" => %{
          "source_addr" => "+12025550100",
          "destination_addr" => "+12025550101",
          "short_message_base64" => Base.encode64("hello")
        }
      }
      |> json()

    assert Envelope.decode(only_base64) == {:error, :invalid_envelope}
  end

  test "rejects v1 JSON-null data_coding" do
    assert Envelope.decode(v1_fixture(%{"data_coding" => :null})) ==
             {:error, :invalid_envelope}
  end

  test "rejects v1 JSON-null registered_delivery" do
    assert Envelope.decode(v1_fixture(%{"registered_delivery" => :null})) ==
             {:error, :invalid_envelope}
  end

  test "rejects new/1 :null data_coding" do
    assert Envelope.new(valid_attributes(data_coding: :null)) == {:error, :invalid_envelope}
  end

  test "rejects new/1 :null registered_delivery" do
    assert Envelope.new(valid_attributes(data_coding: 0, registered_delivery: :null)) ==
             {:error, :invalid_envelope}
  end

  test "v2 defaults absent or JSON-null data_coding and registered_delivery" do
    absent = v2_fixture()
    assert {:ok, envelope} = Envelope.decode(absent)
    assert envelope.submit_sm.data_coding == 0
    assert envelope.submit_sm.registered_delivery == 0
    assert envelope.submit_sm.short_message === "hello"

    nulled =
      v2_fixture(%{
        "data_coding" => :null,
        "registered_delivery" => :null
      })

    assert {:ok, decoded} = Envelope.decode(nulled)
    assert decoded.submit_sm.data_coding == 0
    assert decoded.submit_sm.registered_delivery == 0
  end

  test "rejects malformed v2 shape, types, Base64, and conflicts without raising" do
    atom_name = "untrusted_v2_#{System.unique_integer([:positive])}"

    payloads = [
      v2_wire(%{"submit_sm" => %{"source_addr" => "+1", "destination_addr" => "+2"}}),
      v2_fixture(%{"short_message_base64" => :null}),
      v2_fixture(%{"short_message_base64" => 1}),
      v2_fixture(%{"short_message_base64" => true}),
      v2_fixture(%{"short_message_base64" => []}),
      v2_fixture(%{"short_message_base64" => %{}}),
      v2_fixture(%{"short_message_base64" => "!!!!"}),
      v2_fixture(%{"short_message_base64" => "aGVsbG8"}),
      v2_fixture(%{"short_message_base64" => "aGVsbG8=\n"}),
      v2_fixture(%{"short_message_base64" => "AB=="}),
      v2_fixture(%{"short_message_base64" => "-w=="}),
      v2_fixture(%{"short_message" => "hello"}),
      v2_fixture(%{"short_message" => "hello", "short_message_base64" => "aGVsbG8="}),
      v2_fixture(%{"optional_parameters_base64" => :null}),
      v2_fixture(%{"optional_parameters_base64" => 1}),
      v2_fixture(%{"optional_parameters_base64" => "!!!!"}),
      v2_fixture(%{"optional_parameters_base64" => "aGVsbG8"}),
      v2_fixture(%{"optional_parameters_base64" => "aGVsbG8=\n"}),
      v2_fixture(%{"optional_parameters" => "AA=="}),
      v2_fixture(%{
        "optional_parameters" => "AA==",
        "optional_parameters_base64" => Base.encode64(<<1, 2>>)
      }),
      ~s({"gateway_id":"gateway-1"}),
      "not-json",
      ~s({"version":2,"connector_id":"#{atom_name}"})
    ]

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end

    for payload <- payloads do
      assert Envelope.decode(payload) == {:error, :invalid_envelope}
    end

    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end
  end

  test "unsupported versions stay unsupported_version" do
    assert Envelope.decode(~s({"version":3,"connector_id":"alpha"})) ==
             {:error, :unsupported_version}

    assert Envelope.decode(~s({"version":99})) == {:error, :unsupported_version}
  end

  test "v2 decode keeps top-level unknown fields out of the struct" do
    payload =
      v2_wire(%{
        "extra" => "ignored",
        "evidence" => %{"reason" => "bind_lost"}
      })

    assert {:ok, envelope} = Envelope.decode(payload)
    assert envelope.submit_sm.short_message === "hello"
    refute Map.has_key?(Map.from_struct(envelope), :extra)
    refute Map.has_key?(Map.from_struct(envelope), :evidence)
  end

  test "round-trips binary optional_parameters through v2 canonical Base64" do
    optional = sar_optional()
    attributes = valid_attributes(data_coding: 0, optional_parameters: optional)

    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.optional_parameters === optional
    refute Map.has_key?(envelope.submit_sm, :optional_parameters_base64)

    assert {:ok, encoded} = Envelope.encode(envelope)
    wire = :json.decode(encoded)
    assert wire["version"] === 2
    refute Map.has_key?(wire["submit_sm"], "optional_parameters")
    assert wire["submit_sm"]["optional_parameters_base64"] == Base.encode64(optional)

    assert {:ok, decoded} = Envelope.decode(encoded)
    assert decoded.submit_sm.optional_parameters === optional
    assert decoded == envelope
  end

  test "omits empty optional_parameters so ordinary v2 wire stays unchanged" do
    assert {:ok, bare} = Envelope.new(valid_attributes(data_coding: 0))

    assert {:ok, empty} =
             Envelope.new(valid_attributes(data_coding: 0, optional_parameters: <<>>))

    refute Map.has_key?(bare.submit_sm, :optional_parameters)
    refute Map.has_key?(empty.submit_sm, :optional_parameters)

    assert {:ok, encoded} = Envelope.encode(bare)
    assert {:ok, ^encoded} = Envelope.encode(empty)
    wire = :json.decode(encoded)
    refute Map.has_key?(wire["submit_sm"], "optional_parameters")
    refute Map.has_key?(wire["submit_sm"], "optional_parameters_base64")

    assert {:ok, decoded} = Envelope.decode(v2_fixture(%{"optional_parameters_base64" => ""}))
    refute Map.has_key?(decoded.submit_sm, :optional_parameters)
  end

  test "v1 and v2 missing optional_parameters stay valid without the field" do
    assert {:ok, v1} = Envelope.decode(v1_fixture(%{"short_message" => "hello"}))
    refute Map.has_key?(v1.submit_sm, :optional_parameters)

    assert {:ok, v2} = Envelope.decode(v2_fixture())
    refute Map.has_key?(v2.submit_sm, :optional_parameters)
  end

  test "string-key submit_sm copies optional_parameters" do
    optional = sar_optional()

    attributes = %{
      gateway_id: "gateway-1",
      connector_id: "connector-a",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2026-08-02T15:00:00Z",
      submit_sm: %{
        "source_addr" => "+12025550100",
        "destination_addr" => "+12025550101",
        "short_message" => "hello",
        "optional_parameters" => optional
      }
    }

    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.optional_parameters === optional
  end

  test "rejects non-binary optional_parameters in new/1" do
    for value <- [:null, 1, true, [], %{}, {:sar_msg_ref_num, 42}] do
      attributes = valid_attributes(data_coding: 0, optional_parameters: value)
      assert Envelope.new(attributes) == {:error, :invalid_envelope}
    end
  end

  test "preserves unknown and truncated TLV bytes without PDU validation" do
    unknown = <<0x1403::16, 2::16, 0, 255>>
    truncated = <<1>>

    for optional <- [unknown, truncated] do
      attributes = valid_attributes(data_coding: 0, optional_parameters: optional)
      assert {:ok, envelope} = Envelope.new(attributes)
      assert envelope.submit_sm.optional_parameters === optional
      assert {:ok, encoded} = Envelope.encode(envelope)
      assert {:ok, decoded} = Envelope.decode(encoded)
      assert decoded.submit_sm.optional_parameters === optional
    end
  end

  test "retry rebuild and SubmitSM encode preserve SAR optional parameters" do
    optional = sar_optional()
    attributes = valid_attributes(data_coding: 0, optional_parameters: optional)
    assert {:ok, envelope} = Envelope.new(attributes)

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
    assert next.submit_sm.optional_parameters === optional

    body = struct(Body.SubmitSM, next.submit_sm)
    assert {:ok, wire} = Body.encode(:submit_sm, body)
    assert {:ok, decoded} = Body.decode(:submit_sm, wire)
    assert decoded.optional_parameters === optional
  end

  test "preserves UDHI esm_class and UDH bytes through encode, retry, and SubmitSM" do
    message = udh_short_message()
    attributes = valid_attributes(data_coding: 0, short_message: message, esm_class: 0x40)
    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.esm_class == 0x40
    assert envelope.submit_sm.short_message === message

    assert {:ok, encoded} = Envelope.encode(envelope)
    wire = :json.decode(encoded)
    assert wire["version"] === 2
    assert wire["submit_sm"]["esm_class"] == 0x40
    assert wire["submit_sm"]["short_message_base64"] == Base.encode64(message)

    assert {:ok, decoded} = Envelope.decode(encoded)
    assert decoded.submit_sm.esm_class == 0x40
    assert decoded.submit_sm.short_message === message
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
    assert next.submit_sm.esm_class == 0x40
    assert next.submit_sm.short_message === message

    body = struct(Body.SubmitSM, next.submit_sm)
    assert body.esm_class == 0x40
    assert {:ok, pdu} = Body.encode(:submit_sm, body)
    assert {:ok, decoded_body} = Body.decode(:submit_sm, pdu)
    assert decoded_body.esm_class == 0x40
    assert decoded_body.short_message === message
  end

  test "omitted, explicit 0, and v2 JSON-null esm_class default to 0" do
    assert {:ok, omitted} = Envelope.new(valid_attributes(data_coding: 0))
    refute Map.has_key?(omitted.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, omitted.submit_sm).esm_class == 0

    assert {:ok, zero} = Envelope.new(valid_attributes(data_coding: 0, esm_class: 0))
    refute Map.has_key?(zero.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, zero.submit_sm).esm_class == 0

    string_keys = %{
      gateway_id: "gateway-1",
      connector_id: "connector-a",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2026-08-02T15:00:00Z",
      submit_sm: %{
        "source_addr" => "+12025550100",
        "destination_addr" => "+12025550101",
        "short_message" => "hello"
      }
    }

    assert {:ok, from_string} = Envelope.new(string_keys)
    refute Map.has_key?(from_string.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, from_string.submit_sm).esm_class == 0

    assert {:ok, encoded} = Envelope.encode(omitted)
    assert {:ok, ^encoded} = Envelope.encode(zero)
    wire = :json.decode(encoded)
    refute Map.has_key?(wire["submit_sm"], "esm_class")

    assert {:ok, decoded} = Envelope.decode(encoded)
    refute Map.has_key?(decoded.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, decoded.submit_sm).esm_class == 0

    assert {:ok, nulled} = Envelope.decode(v2_fixture(%{"esm_class" => :null}))
    refute Map.has_key?(nulled.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, nulled.submit_sm).esm_class == 0

    assert {:ok, v1} = Envelope.decode(v1_fixture(%{"short_message" => "hello"}))
    refute Map.has_key?(v1.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, v1.submit_sm).esm_class == 0
  end

  test "string-key submit_sm copies esm_class" do
    message = udh_short_message()

    attributes = %{
      gateway_id: "gateway-1",
      connector_id: "connector-a",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2026-08-02T15:00:00Z",
      submit_sm: %{
        "source_addr" => "+12025550100",
        "destination_addr" => "+12025550101",
        "short_message" => message,
        "esm_class" => 0x40
      }
    }

    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.esm_class == 0x40
    assert envelope.submit_sm.short_message === message
  end

  test "accepts one-octet esm_class and rejects invalid values" do
    assert {:ok, zero} = Envelope.new(valid_attributes(data_coding: 0, esm_class: 0))
    refute Map.has_key?(zero.submit_sm, :esm_class)
    assert {:ok, encoded_zero} = Envelope.encode(zero)
    refute Map.has_key?(:json.decode(encoded_zero)["submit_sm"], "esm_class")
    assert {:ok, decoded_zero} = Envelope.decode(encoded_zero)
    refute Map.has_key?(decoded_zero.submit_sm, :esm_class)
    assert struct(Body.SubmitSM, decoded_zero.submit_sm).esm_class == 0

    for esm_class <- [1, 0x40, 255] do
      assert {:ok, envelope} =
               Envelope.new(valid_attributes(data_coding: 0, esm_class: esm_class))

      assert envelope.submit_sm.esm_class == esm_class
      assert {:ok, encoded} = Envelope.encode(envelope)
      assert {:ok, decoded} = Envelope.decode(encoded)
      assert decoded.submit_sm.esm_class == esm_class
    end

    for value <- [-1, 256, 64.0, :null, true, "64", <<64>>, [], %{}] do
      assert Envelope.new(valid_attributes(data_coding: 0, esm_class: value)) ==
               {:error, :invalid_envelope}
    end

    assert Envelope.decode(v1_fixture(%{"esm_class" => :null})) == {:error, :invalid_envelope}
    assert Envelope.decode(v2_fixture(%{"esm_class" => 256})) == {:error, :invalid_envelope}
    assert Envelope.decode(v2_fixture(%{"esm_class" => "64"})) == {:error, :invalid_envelope}

    v1_literal =
      v1_fixture(%{
        "short_message" => "aGVsbG8=",
        "esm_class" => 0x40
      })

    assert {:ok, v1} = Envelope.decode(v1_literal)
    assert v1.submit_sm.short_message === "aGVsbG8="
    refute v1.submit_sm.short_message === "hello"
    assert v1.submit_sm.esm_class == 0x40
  end

  test "envelope transports UDHI with SAR bytes; SubmitSM encode owns sar_with_udhi" do
    optional = sar_optional()
    message = udh_short_message()

    attributes =
      valid_attributes(
        data_coding: 0,
        short_message: message,
        esm_class: 0x40,
        optional_parameters: optional
      )

    assert {:ok, envelope} = Envelope.new(attributes)
    assert envelope.submit_sm.esm_class == 0x40
    assert envelope.submit_sm.optional_parameters === optional
    assert {:ok, encoded} = Envelope.encode(envelope)
    assert {:ok, decoded} = Envelope.decode(encoded)
    assert decoded.submit_sm.esm_class == 0x40
    assert decoded.submit_sm.optional_parameters === optional
    assert decoded.submit_sm.short_message === message

    body = struct(Body.SubmitSM, decoded.submit_sm)
    assert {:error, {:encode, :sar_with_udhi}} = Body.encode(:submit_sm, body)
  end

  defp valid_attributes(overrides) do
    data_coding = Keyword.fetch!(overrides, :data_coding)
    message = Keyword.get(overrides, :short_message, "hello")

    submit_sm = %{
      source_addr: "+12025550100",
      destination_addr: "+12025550101",
      short_message: message,
      data_coding: data_coding
    }

    submit_sm =
      case Keyword.get(overrides, :registered_delivery) do
        nil -> submit_sm
        value -> Map.put(submit_sm, :registered_delivery, value)
      end

    submit_sm =
      case Keyword.get(overrides, :optional_parameters) do
        nil -> submit_sm
        value -> Map.put(submit_sm, :optional_parameters, value)
      end

    submit_sm =
      case Keyword.get(overrides, :esm_class) do
        nil -> submit_sm
        value -> Map.put(submit_sm, :esm_class, value)
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

  defp binary_payloads do
    [
      {"empty", <<>>},
      {"NUL", <<0>>},
      {"high bytes", <<0x80, 0xFF>>},
      {"invalid UTF-8", <<0xFF, 0xFE>>},
      {"GSM escapes", <<0x1B, 0x14, 0x1B, 0x28>>},
      {"UTF-16BE", <<0x00, 0xE9, 0xD8, 0x3D, 0xDE, 0x00>>},
      {"canonical alphabet", <<0xFB, 0xFF>>},
      {"no size limit", :binary.copy(<<1>>, 255)}
    ]
  end

  defp v1_fixture(submit_overrides) do
    submit =
      %{
        "source_addr" => "+12025550100",
        "destination_addr" => "+12025550101",
        "short_message" => "hello"
      }
      |> Map.merge(submit_overrides)

    %{
      "version" => 1,
      "gateway_id" => "gateway-1",
      "connector_id" => "connector-a",
      "attempt" => 1,
      "max_attempts" => 3,
      "enqueued_at" => "2026-08-01T15:00:00Z",
      "expires_at" => "2026-08-02T15:00:00Z",
      "submit_sm" => submit
    }
    |> json()
  end

  defp v2_fixture(submit_overrides \\ %{}, envelope_overrides \\ %{}) do
    submit =
      %{
        "source_addr" => "+12025550100",
        "destination_addr" => "+12025550101",
        "short_message_base64" => Base.encode64("hello")
      }
      |> Map.merge(submit_overrides)

    v2_wire(Map.put(envelope_overrides, "submit_sm", submit))
  end

  defp v2_wire(overrides) do
    %{
      "version" => 2,
      "gateway_id" => "gateway-1",
      "connector_id" => "connector-a",
      "attempt" => 1,
      "max_attempts" => 3,
      "enqueued_at" => "2026-08-01T15:00:00Z",
      "expires_at" => "2026-08-02T15:00:00Z",
      "submit_sm" => %{
        "source_addr" => "+12025550100",
        "destination_addr" => "+12025550101",
        "short_message_base64" => Base.encode64("hello")
      }
    }
    |> Map.merge(overrides)
    |> json()
  end

  defp json(value), do: value |> :json.encode() |> IO.iodata_to_binary()

  defp sar_optional do
    {:ok, bytes} =
      Tlv.encode(sar_msg_ref_num: 42, sar_total_segments: 2, sar_segment_seqnum: 1)

    bytes
  end

  defp udh_short_message, do: <<5, 0, 3, 42, 2, 1, 255>>
end
