defmodule JasminEx.Messaging.SettlementJournalTest do
  use ExUnit.Case, async: true
  alias JasminEx.Messaging.{SettlementJournal, StateStoreJournal}

  defmodule Store do
    def put(context, key, value, ttl_ms) do
      send(context, {:put, key, value, ttl_ms})
      :ok
    end
  end

  test "quarantines unresolved redelivery and retries only proven not-sent work" do
    dispatching = SettlementJournal.dispatching("gateway-1", 1)

    assert SettlementJournal.redelivery_directive(dispatching) ==
             {:quarantine, %{reason: :unresolved_dispatch}}

    assert {:ok, not_sent} =
             SettlementJournal.record_outcome(dispatching, {:not_sent, %{stage: :pre_write}})

    assert SettlementJournal.redelivery_directive(not_sent) == {:retry, %{stage: :pre_write}}
    assert {:ok, sent} = SettlementJournal.record_outcome(dispatching, {:sent, %{smsc_id: "abc"}})

    assert SettlementJournal.redelivery_directive(sent) ==
             {:quarantine, %{reason: :outcome_already_recorded}}
  end

  test "outcome transitions are monotonic and idempotent" do
    dispatching = SettlementJournal.dispatching("gateway-1", 2)
    outcome = {:not_sent, %{stage: :pre_write}}
    assert {:ok, recorded} = SettlementJournal.record_outcome(dispatching, outcome)
    assert SettlementJournal.record_outcome(recorded, outcome) == {:ok, recorded}

    assert SettlementJournal.record_outcome(recorded, {:sent, %{smsc_id: "abc"}}) ==
             {:error, :outcome_already_recorded}
  end

  test "persists journal records through the StateStore port with a collision-safe key" do
    record = SettlementJournal.dispatching("gateway-1", 7)
    assert StateStoreJournal.write({Store, self()}, record, 60_000) == :ok
    assert_received {:put, key, encoded, 60_000}
    assert key == StateStoreJournal.key("gateway-1", 7)
    assert is_binary(encoded)
  end
end
