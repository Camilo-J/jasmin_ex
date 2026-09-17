defmodule JasminEx.Dlr.EventTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Event
  alias JasminEx.Dlr.Receipt

  describe "submit_sm_resp codec" do
    test "round-trips a successful submit response with a stable event id" do
      event = %{
        kind: :submit_sm_resp,
        gateway_id: "G1",
        connector_id: "c1",
        attempt: 4,
        status: "ESME_ROK",
        raw_smsc_id: "00ab12",
        observed_at_ms: 1_000,
        deadline_ms: 86_401_000
      }

      assert {:ok, binary} = Event.encode(event)
      assert {:ok, decoded} = Event.decode(binary)
      assert decoded.kind == :submit_sm_resp
      assert decoded.gateway_id == "G1"
      assert decoded.connector_id == "c1"
      assert decoded.attempt == 4
      assert decoded.status == "ESME_ROK"
      assert decoded.raw_smsc_id == "00ab12"
      assert decoded.event_id == "c1:G1:4:submit_sm_resp"
      assert decoded.observed_at_ms == 1_000
      assert decoded.deadline_ms == 86_401_000
    end

    test "omits raw_smsc_id when the submit failed" do
      event = %{
        kind: :submit_sm_resp,
        gateway_id: "G1",
        connector_id: "c1",
        attempt: 1,
        status: "ESME_RSUBMITFAIL",
        observed_at_ms: 10,
        deadline_ms: 20
      }

      assert {:ok, binary} = Event.encode(event)
      assert {:ok, decoded} = Event.decode(binary)
      assert decoded.status == "ESME_RSUBMITFAIL"
      assert decoded.raw_smsc_id == nil
    end
  end

  describe "deliver_sm codec" do
    test "round-trips a receipt with hashed event id and normalized SMSC id" do
      receipt = %Receipt{
        id: "00ab12",
        stat: "DELIVRD",
        sub: "001",
        dlvrd: "001",
        sdate: "2601011200",
        ddate: "2601011201",
        err: "000",
        text: "hello"
      }

      event = %{
        kind: :deliver_sm,
        connector_id: "c1",
        receipt: receipt,
        observed_at_ms: 50,
        deadline_ms: 150
      }

      assert {:ok, binary} = Event.encode(event)
      assert {:ok, decoded} = Event.decode(binary)
      assert decoded.kind == :deliver_sm
      assert decoded.connector_id == "c1"
      assert decoded.raw_smsc_id == "00ab12"
      assert decoded.normalized_smsc_id == "AB12"
      assert decoded.status == "DELIVRD"
      assert decoded.sub == "001"
      assert decoded.dlvrd == "001"
      assert decoded.subdate == "2601011200"
      assert decoded.donedate == "2601011201"
      assert decoded.err == "000"
      assert decoded.text == "hello"
      assert decoded.event_id == Event.receipt_event_id("c1", receipt)
      assert decoded.event_id == Event.receipt_event_id("c1", receipt)
    end

    test "receipt event ids ignore SMPP sequence numbers" do
      receipt = %Receipt{id: "AB12", stat: "UNDELIV"}
      first = Event.receipt_event_id("c1", receipt)
      second = Event.receipt_event_id("c1", receipt)
      other = Event.receipt_event_id("c2", receipt)
      assert first == second
      assert first != other
      refute first =~ "seq"
    end
  end

  describe "decode/1" do
    test "rejects an unknown version" do
      binary = ~s({"version":2,"kind":"deliver_sm"})
      assert {:error, :unsupported_version} = Event.decode(binary)
    end

    test "rejects an unknown kind" do
      binary = ~s({"version":1,"kind":"data_sm"})
      assert {:error, :unknown_kind} = Event.decode(binary)
    end
  end
end
