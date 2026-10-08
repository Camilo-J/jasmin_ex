Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Billing.SegmentDispatchTest do
  use ExUnit.Case, async: true

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.SegmentDispatch
  alias JasminEx.Billing.SegmentLedger
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.State
  alias JasminEx.Routing.User

  describe "new/1" do
    test "builds a planned checkpoint from parent binding and ordered children" do
      {:ok, bill} = Bill.new(bill_attrs(segment_count: 2))
      {:ok, fingerprint} = Fingerprint.compute(bill)
      hash_a = hash("seg-a")
      hash_b = hash("seg-b")

      assert {:ok, dispatch} =
               SegmentDispatch.new(
                 bill_id: bill.bill_id,
                 fingerprint: fingerprint,
                 count: 2,
                 children: [
                   %{gateway_id: "1", payload_hash: hash_a},
                   %{gateway_id: "2", payload_hash: hash_b}
                 ]
               )

      assert %SegmentDispatch{} = dispatch
      assert dispatch.bill_id == bill.bill_id
      assert dispatch.fingerprint == fingerprint
      assert %Fingerprint{version: 1, digest: digest} = dispatch.fingerprint
      assert byte_size(digest) == 32
      assert dispatch.count == 2
      assert dispatch.phase == :planned
      assert dispatch.stop_outcome == nil

      assert Enum.map(dispatch.children, &{&1.gateway_id, &1.payload_hash, &1.status}) ==
               [{"1", hash_a, :unattempted}, {"2", hash_b, :unattempted}]

      refute Map.has_key?(dispatch, :outcomes)
      refute Map.has_key?(dispatch, :owner_pid)
      refute Map.has_key?(dispatch, :monitor_ref)
    end

    test "allows child ids equal to count, UDH bytes, or the fingerprint digest" do
      {:ok, fingerprint} = Fingerprint.compute(elem(Bill.new(bill_attrs()), 1))
      digest = fingerprint.digest

      assert {:ok, dispatch} =
               SegmentDispatch.new(
                 bill_id: "bill-1",
                 fingerprint: fingerprint,
                 count: 3,
                 children: [
                   %{gateway_id: "3", payload_hash: hash(1)},
                   %{gateway_id: <<5, 0, 3, 1, 3, 1>>, payload_hash: hash(2)},
                   %{gateway_id: digest, payload_hash: hash(3)}
                 ]
               )

      assert Enum.map(dispatch.children, & &1.gateway_id) == ["3", <<5, 0, 3, 1, 3, 1>>, digest]
    end

    test "rejects invalid fingerprint, count, child ids, hashes, and shape" do
      {:ok, fingerprint} = Fingerprint.compute(elem(Bill.new(bill_attrs()), 1))
      valid_children = [%{gateway_id: "gw-a", payload_hash: hash(1)}]

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: %Fingerprint{version: 2, digest: <<0::256>>},
               count: 1,
               children: valid_children
             ) == {:error, :invalid_fingerprint}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: %Fingerprint{version: 1, digest: <<0, 1, 2>>},
               count: 1,
               children: valid_children
             ) == {:error, :invalid_fingerprint}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: :not_a_fingerprint,
               count: 1,
               children: valid_children
             ) == {:error, :invalid_fingerprint}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 0,
               children: []
             ) == {:error, :invalid_count}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 256,
               children: valid_children
             ) == {:error, :invalid_count}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 2,
               children: valid_children
             ) == {:error, :invalid_count}

      assert SegmentDispatch.new(
               bill_id: "",
               fingerprint: fingerprint,
               count: 1,
               children: valid_children
             ) == {:error, :invalid_bill_id}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: [%{gateway_id: "", payload_hash: hash(1)}]
             ) == {:error, :invalid_gateway_id}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: [%{gateway_id: "bill-1", payload_hash: hash(1)}]
             ) == {:error, :invalid_gateway_id}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 2,
               children: [
                 %{gateway_id: "gw-a", payload_hash: hash(1)},
                 %{gateway_id: "gw-a", payload_hash: hash(2)}
               ]
             ) == {:error, :duplicate_gateway_id}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: [%{gateway_id: "gw-a", payload_hash: <<0, 1, 2>>}]
             ) == {:error, :invalid_payload_hash}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: [%{gateway_id: "gw-a", payload_hash: Base.encode64(hash(1))}]
             ) == {:error, :invalid_payload_hash}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: valid_children,
               phase: :accepted
             ) == {:error, :unsafe_phase}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: valid_children,
               phase: :stopped
             ) == {:error, :unsafe_phase}

      assert SegmentDispatch.new(
               bill_id: "bill-1",
               fingerprint: fingerprint,
               count: 1,
               children: valid_children,
               extra: true
             ) == {:error, :invalid_dispatch}

      assert SegmentDispatch.new(%{}) == {:error, :invalid_dispatch}
      assert SegmentDispatch.new("plan") == {:error, :invalid_dispatch}
    end

    test "rejects unreachable dispatching and stopped child orderings" do
      {:ok, fingerprint} = Fingerprint.compute(elem(Bill.new(bill_attrs(segment_count: 2)), 1))

      assert SegmentDispatch.new(
               dispatch_attrs(fingerprint,
                 phase: :dispatching,
                 children: [
                   %{gateway_id: "gw-a", payload_hash: hash(1), status: :unattempted},
                   %{gateway_id: "gw-b", payload_hash: hash(2), status: :claimed}
                 ]
               )
             ) == {:error, :unsafe_phase}

      assert SegmentDispatch.new(
               dispatch_attrs(fingerprint,
                 phase: :dispatching,
                 children: [
                   %{gateway_id: "gw-a", payload_hash: hash(1), status: :claimed},
                   %{gateway_id: "gw-b", payload_hash: hash(2), status: :claimed}
                 ]
               )
             ) == {:error, :unsafe_phase}

      assert SegmentDispatch.new(
               dispatch_attrs(fingerprint,
                 phase: :dispatching,
                 children: [
                   %{gateway_id: "gw-a", payload_hash: hash(1), status: :claimed},
                   %{gateway_id: "gw-b", payload_hash: hash(2), status: :queued}
                 ]
               )
             ) == {:error, :unsafe_phase}

      assert SegmentDispatch.new(
               dispatch_attrs(fingerprint,
                 phase: :stopped,
                 stop_outcome: :rejected,
                 children: [
                   %{gateway_id: "gw-a", payload_hash: hash(1), status: :failed},
                   %{gateway_id: "gw-b", payload_hash: hash(2), status: :queued}
                 ]
               )
             ) == {:error, :unsafe_phase}

      assert SegmentDispatch.new(
               dispatch_attrs(fingerprint,
                 phase: :stopped,
                 stop_outcome: :rejected,
                 children: [
                   %{gateway_id: "gw-a", payload_hash: hash(1), status: :unattempted},
                   %{gateway_id: "gw-b", payload_hash: hash(2), status: :failed}
                 ]
               )
             ) == {:error, :unsafe_phase}
    end

    test "accepts helper-reachable queued prefix, single claim, and stopped failed shape" do
      dispatch = planned_dispatch(2)
      {:ok, claimed} = SegmentDispatch.claim(dispatch, "gw-a")
      {:ok, queued} = SegmentDispatch.confirm_queued(claimed, "gw-a")
      {:ok, second} = SegmentDispatch.claim(queued, "gw-b")
      {:ok, all_queued} = SegmentDispatch.confirm_queued(second, "gw-b")
      {:ok, stopped} = SegmentDispatch.record_failure(second, "gw-b")

      assert {:ok, ^claimed} = SegmentDispatch.new(attrs_from(claimed))
      assert {:ok, ^queued} = SegmentDispatch.new(attrs_from(queued))
      assert {:ok, ^all_queued} = SegmentDispatch.new(attrs_from(all_queued))
      assert {:ok, ^stopped} = SegmentDispatch.new(attrs_from(stopped))
      assert all_queued.phase == :dispatching
      assert statuses(all_queued) == [:queued, :queued]
    end
  end

  describe "pure publication transitions" do
    test "claims, queues, and fails without treating queue confirm as ledger accepted" do
      dispatch = planned_dispatch(2)

      assert {:ok, claimed} = SegmentDispatch.claim(dispatch, "gw-a")
      assert claimed.phase == :dispatching
      assert statuses(claimed) == [:claimed, :unattempted]
      assert {:error, :invalid_dispatch} = SegmentDispatch.claim(claimed, "gw-b")

      assert {:ok, queued} = SegmentDispatch.confirm_queued(claimed, "gw-a")
      assert queued.phase == :dispatching
      assert statuses(queued) == [:queued, :unattempted]
      refute Enum.any?(queued.children, &(&1.status == :accepted))

      assert {:ok, second} = SegmentDispatch.claim(queued, "gw-b")
      assert {:ok, stopped} = SegmentDispatch.record_failure(second, "gw-b")
      assert stopped.phase == :stopped
      assert stopped.stop_outcome == :rejected
      assert statuses(stopped) == [:queued, :failed]
      assert {:error, :unsafe_phase} = SegmentDispatch.claim(stopped, "gw-b")
    end

    test "recovery refunds only unattempted children and never republishes" do
      dispatch = planned_dispatch(3)
      {:ok, claimed} = SegmentDispatch.claim(dispatch, "gw-a")
      {:ok, queued} = SegmentDispatch.confirm_queued(claimed, "gw-a")
      {:ok, in_flight} = SegmentDispatch.claim(queued, "gw-b")
      {:ok, stopped} = SegmentDispatch.record_failure(in_flight, "gw-b")

      actions = SegmentDispatch.recovery_actions(stopped)

      assert actions == [
               %{gateway_id: "gw-a", action: :hold_queued},
               %{gateway_id: "gw-b", action: :reject},
               %{gateway_id: "gw-c", action: :refund}
             ]

      {:ok, only_claimed} = SegmentDispatch.claim(planned_dispatch(1), "gw-a")

      assert SegmentDispatch.recovery_actions(only_claimed) == [
               %{gateway_id: "gw-a", action: :hold_uncertain}
             ]

      refute Enum.any?(
               actions ++ SegmentDispatch.recovery_actions(only_claimed),
               &(&1.action in [:republish, :accepted])
             )
    end

    test "close keeps reachable children and refuses further claims" do
      dispatch = planned_dispatch(2)
      {:ok, claimed} = SegmentDispatch.claim(dispatch, "gw-a")
      {:ok, queued} = SegmentDispatch.confirm_queued(claimed, "gw-a")
      {:ok, closed} = SegmentDispatch.close(queued, :uncertain)

      assert closed.phase == :closed
      assert closed.stop_outcome == :uncertain
      assert statuses(closed) == [:queued, :unattempted]
      assert {:ok, ^closed} = SegmentDispatch.new(attrs_from(closed))
      assert {:ok, ^closed} = SegmentDispatch.close(closed, :uncertain)
      assert {:error, :unsafe_phase} = SegmentDispatch.claim(closed, "gw-b")
    end
  end

  describe "State.admit_segments_with_dispatch/4" do
    test "debits and attaches a checkpoint bound to the admitted ledger" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])
      before = state

      assert {:ok, %State{} = next} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      reservation = next.reservations["bill-1"]
      dispatch = next.segment_dispatches["bill-1"]

      assert next.users["u1"].balance_minor == 300
      assert next.users["u1"].submit_quota == 3
      assert %SegmentLedger{count: 2} = reservation.ledger
      assert dispatch.bill_id == "bill-1"
      assert dispatch.count == reservation.ledger.count
      assert dispatch.fingerprint == reservation.fingerprint
      assert dispatch.fingerprint == reservation.ledger.fingerprint
      assert Enum.map(dispatch.children, & &1.gateway_id) == ["gw-a", "gw-b"]
      assert before.reservations == %{}
      assert before.segment_dispatches == %{}
    end

    test "standalone admit_segments still debits without a checkpoint" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)

      assert {:ok, %State{} = next} = State.admit_segments(state, admission, clock())
      assert next.users["u1"].balance_minor == 300
      assert next.reservations["bill-1"].ledger.count == 2
      assert next.segment_dispatches == %{}
    end

    test "invalid plan leaves balances and reservations unchanged" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)

      assert {:error, :invalid_gateway_id} =
               State.admit_segments_with_dispatch(
                 state,
                 admission,
                 children_for(admission.bill, ["bill-1", "gw-b"]),
                 clock()
               )

      assert {:error, :invalid_count} =
               State.admit_segments_with_dispatch(
                 state,
                 admission,
                 children_for(admission.bill, ["gw-a"]),
                 clock()
               )

      assert state.users["u1"].balance_minor == 500
      assert state.reservations == %{}
      assert state.segment_dispatches == %{}
    end

    test "does not mint bill ids and keeps caller identity" do
      {state, admission} = fixture(bill_id: "caller-bill", segment_count: 1, submit_quota: 5)
      children = children_for(admission.bill, ["child-1"])

      assert {:ok, next} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      assert Map.keys(next.reservations) == ["caller-bill"]
      assert Map.keys(next.segment_dispatches) == ["caller-bill"]
      assert next.segment_dispatches["caller-bill"].bill_id == admission.bill.bill_id
    end

    test "same bound plan replay is duplicate without a second debit or phase reset" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, admitted} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      assert {:ok, :duplicate} =
               State.admit_segments_with_dispatch(admitted, admission, children, clock())

      assert admitted.users["u1"].balance_minor == 300
      assert admitted.users["u1"].submit_quota == 3
      assert admitted.segment_dispatches["bill-1"].phase == :planned

      {:ok, claimed} = SegmentDispatch.claim(admitted.segment_dispatches["bill-1"], "gw-a")
      claimed_state = put_in(admitted.segment_dispatches["bill-1"], claimed)

      assert {:ok, :duplicate} =
               State.admit_segments_with_dispatch(claimed_state, admission, children, clock())

      assert claimed_state.users["u1"].balance_minor == 300
      assert claimed_state.users["u1"].submit_quota == 3
      assert statuses(claimed_state.segment_dispatches["bill-1"]) == [:claimed, :unattempted]

      {:ok, queued} = SegmentDispatch.confirm_queued(claimed, "gw-a")
      queued_state = put_in(admitted.segment_dispatches["bill-1"], queued)

      assert {:ok, :duplicate} =
               State.admit_segments_with_dispatch(queued_state, admission, children, clock())

      assert statuses(queued_state.segment_dispatches["bill-1"]) == [:queued, :unattempted]

      {:ok, second} = SegmentDispatch.claim(queued, "gw-b")
      {:ok, stopped} = SegmentDispatch.record_failure(second, "gw-b")
      stopped_state = put_in(admitted.segment_dispatches["bill-1"], stopped)

      assert {:ok, :duplicate} =
               State.admit_segments_with_dispatch(stopped_state, admission, children, clock())

      assert stopped_state.segment_dispatches["bill-1"].phase == :stopped
      assert statuses(stopped_state.segment_dispatches["bill-1"]) == [:queued, :failed]
    end

    test "changed fingerprint, child ids, or payload hash fail closed without debit" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, admitted} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      other_ids = children_for(admission.bill, ["gw-x", "gw-b"])

      assert {:error, :billing_conflict} =
               State.admit_segments_with_dispatch(admitted, admission, other_ids, clock())

      tampered = [
        %{hd(children) | payload_hash: hash(:tampered)},
        List.last(children)
      ]

      assert {:error, :billing_conflict} =
               State.admit_segments_with_dispatch(admitted, admission, tampered, clock())

      {:ok, other_bill} = Bill.new(bill_attrs(segment_count: 2, rate_minor: 50))
      {:ok, other_admission} = Admission.new(bill: other_bill, ttl_ms: 1_000)

      assert {:error, :billing_conflict} =
               State.admit_segments_with_dispatch(admitted, other_admission, children, clock())

      assert admitted.users["u1"].balance_minor == 300
      assert admitted.users["u1"].submit_quota == 3
      assert map_size(admitted.reservations) == 1
      assert statuses(admitted.segment_dispatches["bill-1"]) == [:unattempted, :unattempted]
    end

    test "preexisting reservation or tombstone without this checkpoint blocks retrofit" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, open} = State.admit_segments(state, admission, clock())

      assert {:error, :billing_conflict} =
               State.admit_segments_with_dispatch(open, admission, children, clock())

      assert open.users["u1"].balance_minor == 300
      assert open.users["u1"].submit_quota == 3
      assert open.segment_dispatches == %{}
      assert open.reservations["bill-1"].ledger.count == 2

      assert {:ok, accepted} =
               State.settle_segment(
                 open,
                 admission.bill.bill_id,
                 open.reservations["bill-1"].fingerprint,
                 1,
                 :accepted
               )

      assert {:ok, closed} =
               State.settle_segment(
                 accepted,
                 admission.bill.bill_id,
                 open.reservations["bill-1"].fingerprint,
                 2,
                 :accepted
               )

      assert closed.tombstones["bill-1"].state == :settled_ok

      assert {:error, :billing_conflict} =
               State.admit_segments_with_dispatch(closed, admission, children, clock())

      assert closed.segment_dispatches == %{}
      assert closed.reservations == %{}
      assert closed.users["u1"].balance_minor == 300
    end

    test "recover refunds unpublished children and closes without republishing" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, admitted} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      {:ok, claimed} = SegmentDispatch.claim(admitted.segment_dispatches["bill-1"], "gw-a")
      claimed_state = put_in(admitted.segment_dispatches["bill-1"], claimed)

      assert {:ok, recovered} = State.recover_dispatch(claimed_state, "bill-1")
      assert recovered.users["u1"].balance_minor == 400
      assert recovered.users["u1"].submit_quota == 4
      assert recovered.segment_dispatches["bill-1"].phase == :closed
      assert recovered.reservations["bill-1"].ledger.outcomes[1] == :uncertain
      assert recovered.reservations["bill-1"].ledger.outcomes[2] == :rejected
      assert {:ok, :duplicate} = State.recover_dispatch(recovered, "bill-1")
      assert recovered.users["u1"].balance_minor == 400
    end

    test "recover preserves a claimed child already accepted and refunds only the suffix" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, admitted} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      {:ok, claimed} = SegmentDispatch.claim(admitted.segment_dispatches["bill-1"], "gw-a")
      claimed_state = put_in(admitted.segment_dispatches["bill-1"], claimed)
      fingerprint = claimed_state.reservations["bill-1"].fingerprint

      assert {:ok, accepted} =
               State.settle_segment(claimed_state, "bill-1", fingerprint, 1, :accepted)

      assert accepted.users["u1"].balance_minor == 300
      assert accepted.reservations["bill-1"].ledger.outcomes[1] == :accepted
      assert {:ok, recovered} = State.recover_dispatch(accepted, "bill-1")
      assert recovered.users["u1"].balance_minor == 400
      assert recovered.users["u1"].submit_quota == 4
      assert recovered.segment_dispatches["bill-1"].phase == :closed
      assert recovered.tombstones["bill-1"]
      refute Map.has_key?(recovered.reservations, "bill-1")
      assert {:ok, :duplicate} = State.recover_dispatch(recovered, "bill-1")
      assert recovered.users["u1"].balance_minor == 400
    end

    test "recover does not duplicate a claimed child already rejected" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, admitted} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      {:ok, claimed} = SegmentDispatch.claim(admitted.segment_dispatches["bill-1"], "gw-a")
      claimed_state = put_in(admitted.segment_dispatches["bill-1"], claimed)
      fingerprint = claimed_state.reservations["bill-1"].fingerprint

      assert {:ok, rejected} =
               State.settle_segment(claimed_state, "bill-1", fingerprint, 1, :rejected)

      assert rejected.users["u1"].balance_minor == 400
      assert rejected.reservations["bill-1"].ledger.outcomes[1] == :rejected
      assert {:ok, recovered} = State.recover_dispatch(rejected, "bill-1")
      assert recovered.users["u1"].balance_minor == 500
      assert recovered.users["u1"].submit_quota == 5
      assert recovered.segment_dispatches["bill-1"].phase == :closed
      assert {:ok, :duplicate} = State.recover_dispatch(recovered, "bill-1")
      assert recovered.users["u1"].balance_minor == 500
    end

    test "recover of a mismatched reservation fingerprint fails closed" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      children = children_for(admission.bill, ["gw-a", "gw-b"])

      assert {:ok, admitted} =
               State.admit_segments_with_dispatch(state, admission, children, clock())

      {:ok, claimed} = SegmentDispatch.claim(admitted.segment_dispatches["bill-1"], "gw-a")
      claimed_state = put_in(admitted.segment_dispatches["bill-1"], claimed)
      {:ok, other_bill} = Bill.new(bill_attrs(segment_count: 2, rate_minor: 50))
      {:ok, other_fingerprint} = Fingerprint.compute(other_bill)

      mismatched =
        update_in(claimed_state.reservations["bill-1"], fn reservation ->
          %{reservation | fingerprint: other_fingerprint}
        end)

      assert {:error, :billing_conflict} = State.recover_dispatch(mismatched, "bill-1")
      assert mismatched.users["u1"].balance_minor == 300
    end

    test "recover of a standalone reservation without a checkpoint does not refund" do
      {state, admission} = fixture(segment_count: 2, submit_quota: 5)
      assert {:ok, open} = State.admit_segments(state, admission, clock())
      assert {:error, :unknown_bill} = State.recover_dispatch(open, "bill-1")
      assert open.users["u1"].balance_minor == 300
      assert open.reservations["bill-1"].ledger.outcomes == %{}
    end
  end

  defp dispatch_attrs(fingerprint, opts) do
    [
      bill_id: "bill-1",
      fingerprint: fingerprint,
      count: 2,
      phase: Keyword.fetch!(opts, :phase),
      stop_outcome: Keyword.get(opts, :stop_outcome),
      children: Keyword.fetch!(opts, :children)
    ]
  end

  defp attrs_from(dispatch) do
    [
      bill_id: dispatch.bill_id,
      fingerprint: dispatch.fingerprint,
      count: dispatch.count,
      phase: dispatch.phase,
      stop_outcome: dispatch.stop_outcome,
      children:
        Enum.map(dispatch.children, fn child ->
          %{
            gateway_id: child.gateway_id,
            payload_hash: child.payload_hash,
            status: child.status
          }
        end)
    ]
  end

  defp planned_dispatch(count) do
    ids = Enum.map(1..count, fn n -> "gw-" <> <<96 + n>> end)
    {:ok, fingerprint} = Fingerprint.compute(elem(Bill.new(bill_attrs(segment_count: count)), 1))

    {:ok, dispatch} =
      SegmentDispatch.new(
        bill_id: "bill-1",
        fingerprint: fingerprint,
        count: count,
        children:
          Enum.with_index(ids, 1)
          |> Enum.map(fn {id, index} -> %{gateway_id: id, payload_hash: hash(index)} end)
      )

    dispatch
  end

  defp statuses(dispatch), do: Enum.map(dispatch.children, & &1.status)

  defp children_for(%Bill{} = bill, ids) do
    Enum.with_index(ids, 1)
    |> Enum.map(fn {id, index} ->
      %{gateway_id: id, payload_hash: hash({bill.bill_id, index})}
    end)
  end

  defp bill_attrs(opts \\ []) do
    [
      bill_id: Keyword.get(opts, :bill_id, "bill-1"),
      uid: "u1",
      route_order: 10,
      rate_minor: Keyword.get(opts, :rate_minor, 100),
      precharge_percent: 10,
      segment_count: Keyword.get(opts, :segment_count, 1)
    ]
  end

  defp hash(term), do: :crypto.hash(:sha256, :erlang.term_to_binary(term))

  defp clock, do: {FakeClock, FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)}

  defp fixture(opts) do
    {:ok, group} = Group.new(gid: "ops")

    {:ok, user} =
      User.new(
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: Keyword.get(opts, :balance_minor, 500),
        submit_quota: Keyword.get(opts, :submit_quota, 3)
      )

    {:ok, connector} = ConnectorRef.new("smpp-t")

    {:ok, route} =
      Route.new(
        kind: :static,
        order: 10,
        connector: connector,
        filters: [],
        rate_minor: 100,
        precharge_percent: 10
      )

    {:ok, state} = State.put_group(State.new(), group)
    {:ok, state} = State.put_user(state, user)
    {:ok, state} = State.put_route(state, route)
    {:ok, bill} = Bill.new(bill_attrs(opts))
    {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1_000)
    {state, admission}
  end
end
