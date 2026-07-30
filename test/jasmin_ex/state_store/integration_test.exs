defmodule JasminEx.StateStore.IntegrationTest do
  use ExUnit.Case, async: false

  alias JasminEx.StateStore
  alias JasminEx.StateStoreHarness

  setup_all do
    harness = StateStoreHarness.start!()
    on_exit(fn -> StateStoreHarness.stop!(harness) end)
    %{harness: harness}
  end

  test "round-trips binary values and keeps missing keys distinct", %{harness: harness} do
    store = StateStoreHarness.store(harness)
    key = <<0, 255, "key">>
    value = <<255, 0, "value">>

    assert StateStore.fetch(store, key) == :missing
    assert StateStore.put(store, key, value, 1_000) == :ok
    assert StateStore.fetch(store, key) == {:ok, value}
  end

  test "overwrites values and reports confirmed delete outcomes", %{harness: harness} do
    store = StateStoreHarness.store(harness)
    key = "overwrite-delete"

    assert StateStore.put(store, key, "first", 1_000) == :ok
    assert StateStore.put(store, key, "second", 1_000) == :ok
    assert StateStore.fetch(store, key) == {:ok, "second"}
    assert StateStoreHarness.eventually(fn -> StateStore.fetch(store, key) == :missing end)
    assert StateStore.put(store, key, "delete", 1_000) == :ok
    assert StateStore.delete(store, key) == :deleted
    assert StateStore.delete(store, key) == :missing
  end

  test "expires a short-lived value through bounded polling", %{harness: harness} do
    store = StateStoreHarness.store(harness)

    assert StateStore.put(store, "expires", "value", 50) == :ok
    assert StateStoreHarness.eventually(fn -> StateStore.fetch(store, "expires") == :missing end)
  end

  test "authenticates configured connections", %{harness: harness} do
    store = StateStoreHarness.store(harness)

    assert StateStore.put(store, "authenticated", "value", 1_000) == :ok
    assert StateStore.fetch(store, "authenticated") == {:ok, "value"}
  end

  test "returns explicit failures during an outage and recovers after restart", %{
    harness: harness
  } do
    store = StateStoreHarness.store(harness)

    assert StateStore.put(store, "recovery", "value", 1_000) == :ok
    :ok = StateStoreHarness.stop_valkey!(harness)
    assert match?({:error, {:store, _reason}}, StateStore.fetch(store, "recovery"))
    :ok = StateStoreHarness.start_valkey!(harness)

    assert StateStoreHarness.eventually(fn ->
             StateStore.put(store, "recovery", "recovered", 1_000) == :ok
           end)

    assert StateStore.fetch(store, "recovery") == {:ok, "recovered"}
  end

  test "starts a reconnecting client while Valkey is unavailable", %{harness: harness} do
    :ok = StateStoreHarness.stop_valkey!(harness)
    store = StateStoreHarness.store(harness)

    assert match?({:error, {:store, _reason}}, StateStore.fetch(store, "startup"))
    :ok = StateStoreHarness.start_valkey!(harness)

    assert StateStoreHarness.eventually(fn ->
             StateStore.put(store, "startup", "recovered", 1_000) == :ok
           end)
  end
end
