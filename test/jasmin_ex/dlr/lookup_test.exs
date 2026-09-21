defmodule JasminEx.Dlr.LookupTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Lookup

  @request %{
    gateway_id: "G1",
    connector_id: "c1",
    url: "https://example.com/dlr",
    level: 1,
    method: "POST",
    expiry_s: 86_400,
    expires_at_ms: 90_000
  }

  test "level 1 submit success creates a compatible callback and cleanup" do
    event = submit_event()

    assert {:ok, plan} = Lookup.plan(event, request: {:ok, @request})
    assert plan.event_id == event.event_id
    assert plan.reverse == nil
    assert plan.cleanup == {:request, "G1"}
    assert plan.job.level == 1

    assert plan.job.fields == %{
             "connector" => "c1",
             "id" => "G1",
             "level" => "1",
             "message_status" => "ESME_ROK"
           }
  end

  test "level 2 submit success creates only the reverse mapping" do
    request = %{@request | level: 2}

    assert {:ok, plan} = Lookup.plan(submit_event(), request: {:ok, request})
    assert plan.job == nil
    assert plan.cleanup == nil
    assert plan.reverse.gateway_id == "G1"
    assert plan.reverse.raw_smsc_id == "00ab12"
    assert plan.reverse.expiry_s == 86_400
  end

  test "level 3 submit success creates level 1 callback and reverse mapping" do
    request = %{@request | level: 3}

    assert {:ok, plan} = Lookup.plan(submit_event(), request: {:ok, request})
    assert plan.job.level == 1
    assert plan.reverse.raw_smsc_id == "00ab12"
    assert plan.cleanup == nil
  end

  test "failed submit only produces level 1 callback for requested levels 1 and 3" do
    event = %{submit_event() | status: "ESME_RSUBMITFAIL", raw_smsc_id: nil}

    for level <- [1, 3] do
      assert {:ok, plan} = Lookup.plan(event, request: {:ok, %{@request | level: level}})
      assert plan.job.fields["message_status"] == "ESME_RSUBMITFAIL"
      assert plan.cleanup == {:request, "G1"}
      assert plan.reverse == nil
    end

    assert {:terminal, :submit_failed_without_callback} =
             Lookup.plan(event, request: {:ok, %{@request | level: 2}})
  end

  test "submit request with an unsupported level is terminal instead of crashing" do
    assert {:terminal, {:malformed_map, :invalid_state}} =
             Lookup.plan(submit_event(), request: {:ok, %{@request | level: 4}})
  end

  test "final receipt creates level 2 callback using Python-compatible connector fields" do
    event = receipt_event()
    reverse = {:ok, %{gateway_id: "G1", connector_id: "c1"}}
    request = {:ok, %{@request | level: 3}}

    assert {:ok, plan} = Lookup.plan(event, reverse: reverse, request: request)
    assert plan.cleanup == {:request, "G1"}
    assert plan.job.level == 2
    assert plan.job.fields["connector"] == "00ab12"
    assert plan.job.fields["id_smsc"] == "AB12"
    assert plan.job.fields["id"] == "G1"
    assert plan.job.fields["message_status"] == "DELIVRD"
  end

  test "non-final receipt keeps correlation and level 1 requests do not receive receipts" do
    event = %{receipt_event() | status: "ACCEPTD"}
    reverse = {:ok, %{gateway_id: "G1", connector_id: "c1"}}

    assert {:ok, plan} =
             Lookup.plan(event, reverse: reverse, request: {:ok, %{@request | level: 2}})

    assert plan.cleanup == nil
    assert plan.job.level == 2

    assert {:terminal, :receipt_not_requested} =
             Lookup.plan(event, reverse: reverse, request: {:ok, @request})
  end

  test "missing maps and operational failures are classified by event kind" do
    assert {:terminal, :submit_map_missing} = Lookup.plan(submit_event(), request: :missing)
    assert {:retry, :reverse_map_missing} = Lookup.plan(receipt_event(), reverse: :missing)

    assert {:retry, {:store, :unavailable}} =
             Lookup.plan(submit_event(), request: {:error, :unavailable})

    assert {:terminal, {:malformed_map, :invalid_state}} =
             Lookup.plan(submit_event(), request: {:error, {:malformed_map, :invalid_state}})
  end

  test "mismatched connector and expired event are terminal" do
    assert {:terminal, :connector_mismatch} =
             Lookup.plan(submit_event(), request: {:ok, %{@request | connector_id: "c2"}})

    assert {:terminal, :expired} =
             Lookup.plan(%{submit_event() | deadline_ms: 999},
               request: {:ok, @request},
               now_ms: 1_000
             )
  end

  defp submit_event do
    %{
      kind: :submit_sm_resp,
      event_id: "event-submit",
      gateway_id: "G1",
      connector_id: "c1",
      status: "ESME_ROK",
      raw_smsc_id: "00ab12",
      observed_at_ms: 1_000,
      deadline_ms: 80_000
    }
  end

  defp receipt_event do
    %{
      kind: :deliver_sm,
      event_id: "event-receipt",
      connector_id: "c1",
      raw_smsc_id: "00ab12",
      normalized_smsc_id: "AB12",
      status: "DELIVRD",
      sub: "001",
      dlvrd: "001",
      subdate: "2601011200",
      donedate: "2601011201",
      err: "000",
      text: "hello",
      observed_at_ms: 2_000,
      deadline_ms: 80_000
    }
  end
end
