defmodule JasminEx.StateStore.RedixTest do
  use ExUnit.Case, async: true

  alias JasminEx.StateStore
  alias JasminEx.StateStore.Config
  alias JasminEx.StateStore.Redix, as: StateStoreRedix

  @namespace {"test", "v1"}

  test "maps GET binary and missing replies without exposing the command result" do
    assert StateStoreRedix.fetch(context({:ok, <<0, 255>>}), "key") == {:ok, <<0, 255>>}
    assert_received {:command, ["GET", _physical_key], 25}

    assert StateStoreRedix.fetch(context({:ok, nil}), "key") == :missing
    assert_received {:command, ["GET", _physical_key], 25}
  end

  test "maps confirmed SET and DEL replies and issues each mutation once" do
    assert StateStoreRedix.put(context({:ok, "OK"}), "key", <<0>>, 50) == :ok
    assert_received {:command, ["SET", _physical_key, <<0>>, "PX", "50"], 25}
    refute_received {:command, _command, _timeout}

    assert StateStoreRedix.delete(context({:ok, 1}), "key") == :deleted
    assert_received {:command, ["DEL", _physical_key], 25}
    refute_received {:command, _command, _timeout}

    assert StateStoreRedix.delete(context({:ok, 0}), "key") == :missing
    assert_received {:command, ["DEL", _physical_key], 25}
  end

  test "keeps unavailable and protocol failures confirmed while mutation timeouts are ambiguous" do
    assert StateStoreRedix.fetch(context({:error, :noproc}), "key") ==
             {:error, {:store, :unavailable}}

    assert_received {:command, ["GET", _physical_key], 25}

    assert StateStoreRedix.fetch(context({:ok, :unexpected}), "key") ==
             {:error, {:store, :protocol}}

    assert_received {:command, ["GET", _physical_key], 25}

    assert StateStoreRedix.put(context({:error, :noproc}), "key", "value", 1) ==
             {:error, {:store, :unavailable}}

    assert_received {:command, ["SET", _physical_key, "value", "PX", "1"], 25}

    assert StateStoreRedix.delete(context({:ok, 2}), "key") == {:error, {:store, :protocol}}

    assert_received {:command, ["DEL", _physical_key], 25}

    assert StateStoreRedix.put(context({:error, :timeout}), "key", "value", 1) ==
             {:ambiguous, {:store, :timeout}}

    assert_received {:command, ["SET", _physical_key, "value", "PX", "1"], 25}
    refute_received {:command, _command, _timeout}

    assert StateStoreRedix.delete(context({:error, :disconnected}), "key") ==
             {:ambiguous, {:store, :disconnected}}

    assert_received {:command, ["DEL", _physical_key], 25}
    refute_received {:command, _command, _timeout}

    assert StateStoreRedix.put(context({:error, :health_check_timeout}), "key", "value", 1) ==
             {:ambiguous, {:store, :health_check_timeout}}

    assert_received {:command, ["SET", _physical_key, "value", "PX", "1"], 25}
    refute_received {:command, _command, _timeout}
  end

  test "executes adapter commands with the configured command timeout" do
    test_process = self()

    context =
      Config.new!(command_timeout_ms: 37)
      |> StateStoreRedix.context()
      |> Map.put(:command, fn connection, command, timeout ->
        send(test_process, {:command, connection, command, timeout})
        {:ok, nil}
      end)

    assert StateStoreRedix.fetch(context, "key") == :missing
    assert_received {:command, JasminEx.StateStore.Connection, ["GET", _physical_key], 37}
  end

  test "normalizes Redix error structs through the public facade" do
    rejected = %Redix.Error{message: "WRONGTYPE"}
    disconnected = %Redix.ConnectionError{reason: :disconnected}
    store = {StateStoreRedix, context({:error, rejected})}

    assert StateStore.fetch(store, "key") == {:error, {:store, :rejected}}
    assert StateStore.put(store, "key", "value", 1) == {:error, {:store, :rejected}}

    assert StateStore.delete({StateStoreRedix, context({:error, disconnected})}, "key") ==
             {:ambiguous, {:store, :disconnected}}
  end

  defp context(result) do
    %{
      connection: self(),
      command_timeout_ms: 25,
      key_namespace: @namespace,
      command: fn connection, command, timeout ->
        send(connection, {:command, command, timeout})
        result
      end
    }
  end
end
