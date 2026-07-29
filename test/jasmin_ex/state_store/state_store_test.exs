defmodule JasminEx.StateStoreTest do
  use ExUnit.Case, async: true

  alias JasminEx.StateStore

  defmodule Store do
    def fetch(context, key), do: send(context, {:fetch, key})
    def put(context, key, value, ttl), do: send(context, {:put, key, value, ttl})
    def delete(context, key), do: send(context, {:delete, key})
  end

  test "dispatches valid binary operations with opaque context" do
    store = {Store, self()}
    assert StateStore.fetch(store, <<255>>) == {:fetch, <<255>>}
    assert StateStore.put(store, "key", <<0>>, 1) == {:put, "key", <<0>>, 1}
    assert StateStore.delete(store, "key") == {:delete, "key"}
  end

  test "rejects invalid keys, values, and explicit TTLs before dispatch" do
    store = {Store, self()}
    assert StateStore.fetch(store, :key) == {:error, {:invalid_argument, :key}}
    assert StateStore.put(store, "key", :value, 1) == {:error, {:invalid_argument, :value}}

    for ttl <- [nil, 0, -1, 1.5] do
      assert StateStore.put(store, "key", "value", ttl) == {:error, {:invalid_argument, :ttl_ms}}
    end

    refute_received _message
  end
end
