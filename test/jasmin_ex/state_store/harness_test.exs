defmodule JasminEx.StateStoreHarnessTest do
  use ExUnit.Case, async: true

  alias JasminEx.StateStoreHarness

  test "selects an available non-default port for a harness run" do
    harness = StateStoreHarness.new()

    assert harness.port in 1_024..65_535
    refute harness.port == 6_397

    assert {:ok, socket} = :gen_tcp.listen(harness.port, [:binary, active: false])
    :ok = :gen_tcp.close(socket)
  end

  test "passes its selected port to every Compose command" do
    harness = StateStoreHarness.new()

    assert {"STATE_STORE_TEST_PORT", Integer.to_string(harness.port)} in StateStoreHarness.compose_environment(
             harness
           )
  end
end
