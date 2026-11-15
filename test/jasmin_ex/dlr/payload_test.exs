defmodule JasminEx.Dlr.PayloadTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Payload

  @request %{gateway_id: "G1", connector_id: "smpp-t"}

  describe "classify/1" do
    test "DELIVRD is delivered" do
      assert Payload.classify("DELIVRD") == :delivered
    end

    test "UNDELIV, REJECTD, EXPIRED and DELETED are definitive failures" do
      for status <- ~w(UNDELIV REJECTD EXPIRED DELETED) do
        assert Payload.classify(status) == :failed
      end
    end

    test "any other status is intermediate" do
      for status <- ~w(ENROUTE ACCEPTD UNKNOWN delivrd) do
        assert Payload.classify(status) == :intermediate
      end
    end
  end

  describe "acceptance/2" do
    test "a successful submit response is ACCEPTED with the provider code preserved" do
      event = %{status: "ESME_ROK", raw_smsc_id: "abc123"}

      assert Payload.acceptance(@request, event) == %{
               "schema_version" => 1,
               "id" => "G1",
               "level" => 1,
               "status" => "ACCEPTED",
               "status_origin" => "gateway",
               "connector" => "smpp-t",
               "total_segments" => 1,
               "segments" => [
                 %{"index" => 1, "id_smsc" => "abc123", "message_status" => "ESME_ROK"}
               ]
             }
    end

    test "a rejected submit is FAILED and omits the unknown SMSC id" do
      event = %{status: "ESME_RSUBMITFAIL", raw_smsc_id: nil}
      payload = Payload.acceptance(@request, event)

      assert payload["status"] == "FAILED"
      assert payload["segments"] == [%{"index" => 1, "message_status" => "ESME_RSUBMITFAIL"}]
      refute Map.has_key?(payload, "reason")
    end
  end

  describe "delivery/3" do
    test "matches the agreed single-segment shape" do
      event = receipt(%{observed_at_ms: 1_760_487_610_120})

      assert Payload.delivery(@request, event, "DELIVERED") == %{
               "schema_version" => 1,
               "id" => "G1",
               "level" => 2,
               "status" => "DELIVERED",
               "status_origin" => "gateway",
               "connector" => "smpp-t",
               "total_segments" => 1,
               "segments" => [
                 %{
                   "index" => 1,
                   "id_smsc" => "abc123",
                   "message_status" => "DELIVRD",
                   "err" => "000",
                   "sub" => "001",
                   "dlvrd" => "001",
                   "subdate" => "2601011200",
                   "donedate" => "2601011201",
                   "text" => "hello",
                   "received_at" => "2025-10-15T00:20:10.120Z"
                 }
               ]
             }
    end

    test "ND placeholders and empty text are omitted as missing evidence" do
      event = receipt(%{status: "UNDELIV", err: "ND", sub: "ND", text: ""})
      [segment] = Payload.delivery(@request, event, "FAILED")["segments"]

      assert segment["message_status"] == "UNDELIV"
      refute Map.has_key?(segment, "err")
      refute Map.has_key?(segment, "sub")
      refute Map.has_key?(segment, "text")
      assert segment["dlvrd"] == "001"
    end

    test "the root connector is the submitting connector, never the raw SMSC id" do
      payload = Payload.delivery(@request, receipt(%{}), "DELIVERED")

      assert payload["connector"] == "smpp-t"
      refute Map.has_key?(payload, "id_smsc")
      refute Map.has_key?(payload, "message_status")
    end
  end

  defp receipt(overrides) do
    Map.merge(
      %{
        raw_smsc_id: "abc123",
        status: "DELIVRD",
        err: "000",
        sub: "001",
        dlvrd: "001",
        subdate: "2601011200",
        donedate: "2601011201",
        text: "hello",
        observed_at_ms: 1_000
      },
      overrides
    )
  end
end
