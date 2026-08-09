defmodule JasminEx.Messaging.SettlementJournalTest do
  use ExUnit.Case, async: true
  alias JasminEx.Messaging.{SettlementJournal, StateStoreJournal}

  defmodule Store do
    def put(table, key, value, ttl_ms) do
      true = :ets.insert(table, {key, value, ttl_ms})
      :ok
    end

    def fetch(table, key) do
      case :ets.lookup(table, key) do
        [{^key, value, _ttl_ms}] -> {:ok, value}
        [] -> :missing
      end
    end
  end

  setup do
    table = :ets.new(:settlement_journal_store, [:set, :public])
    %{store: {Store, table}}
  end

  test "quarantines unresolved redelivery and retries only proven not-sent work" do
    dispatching = SettlementJournal.dispatching("gateway-1", 1)

    assert SettlementJournal.redelivery_directive(dispatching) ==
             {:quarantine, %{"reason" => "unresolved_dispatch"}}

    assert {:ok, not_sent} =
             SettlementJournal.record_outcome(dispatching, {:not_sent, %{stage: :pre_write}})

    assert not_sent.evidence == %{"stage" => "pre_write"}
    assert SettlementJournal.redelivery_directive(not_sent) == {:retry, %{"stage" => "pre_write"}}
    assert {:ok, sent} = SettlementJournal.record_outcome(dispatching, {:sent, %{smsc_id: "abc"}})
    assert sent.evidence == %{"smsc_id" => "abc"}

    assert SettlementJournal.redelivery_directive(sent) ==
             {:quarantine, %{"reason" => "outcome_already_recorded"}}
  end

  test "outcome transitions are monotonic and idempotent" do
    dispatching = SettlementJournal.dispatching("gateway-1", 2)
    outcome = {:not_sent, %{stage: :pre_write}}
    assert {:ok, recorded} = SettlementJournal.record_outcome(dispatching, outcome)
    assert SettlementJournal.record_outcome(recorded, outcome) == {:ok, recorded}

    assert SettlementJournal.record_outcome(recorded, {:sent, %{smsc_id: "abc"}}) ==
             {:error, :outcome_already_recorded}
  end

  test "persists journal records through the StateStore port with a collision-safe key", %{
    store: store
  } do
    record = SettlementJournal.dispatching("gateway-1", 7)
    assert StateStoreJournal.write(store, record, 60_000) == :ok
    assert {:ok, ^record} = StateStoreJournal.read(store, "gateway-1", 7)

    {Store, table} = store
    assert [{key, _encoded, 60_000}] = :ets.tab2list(table)
    assert key == StateStoreJournal.key("gateway-1", 7)
    assert key != StateStoreJournal.key("gateway-10", 7)
    assert key != StateStoreJournal.key("gateway-1", 70)
  end

  test "round-trips canonical evidence without changing shape or breaking idempotency", %{
    store: store
  } do
    dispatching = SettlementJournal.dispatching("gateway-1", 3)

    assert {:ok, recorded} =
             SettlementJournal.record_outcome(dispatching, {:not_sent, %{stage: :pre_write}})

    assert recorded.evidence == %{"stage" => "pre_write"}
    assert StateStoreJournal.write(store, recorded, 60_000) == :ok
    assert {:ok, reloaded} = StateStoreJournal.read(store, "gateway-1", 3)
    assert reloaded == recorded

    assert SettlementJournal.record_outcome(reloaded, {:not_sent, %{stage: :pre_write}}) ==
             {:ok, reloaded}

    assert SettlementJournal.record_outcome(reloaded, {:not_sent, %{"stage" => "pre_write"}}) ==
             {:ok, reloaded}
  end

  test "read rejects invalid stored payloads and propagates fetch miss or error", %{store: store} do
    key = StateStoreJournal.key("gateway-1", 9)
    {Store, table} = store
    true = :ets.insert(table, {key, ~s({"not":"a-journal-record"}), 60_000})

    assert StateStoreJournal.read(store, "gateway-1", 9) == {:error, :invalid_journal_record}
    assert StateStoreJournal.read(store, "gateway-missing", 1) == :missing

    error_store =
      {__MODULE__.ErrorStore, {:error, {:store, :unavailable}}}

    assert StateStoreJournal.read(error_store, "gateway-1", 1) ==
             {:error, {:store, :unavailable}}
  end

  defmodule ErrorStore do
    def fetch(result, _key), do: result
  end
end
