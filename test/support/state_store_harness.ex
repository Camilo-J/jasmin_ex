defmodule JasminEx.StateStoreHarness do
  @moduledoc false

  alias JasminEx.StateStore.Redix, as: StateStoreRedix

  @compose_file "compose.state-store.yml"
  @password "state-store-test-password"
  @command_timeout_ms 250

  def start! do
    harness = %{project: "jasmin-ex-state-store-#{System.unique_integer([:positive])}"}

    try do
      :ok = start_valkey!(harness)
      Map.put(harness, :port, port!(harness))
    rescue
      error ->
        try do
          stop!(harness)
        rescue
          _cleanup_error -> :ok
        end

        reraise error, __STACKTRACE__
    end
  end

  def stop!(harness), do: run!(harness, ["down", "--volumes", "--remove-orphans"])
  def stop_valkey!(harness), do: run!(harness, ["stop", "valkey"])
  def start_valkey!(harness), do: run!(harness, ["up", "--wait", "--no-deps", "valkey"])

  def store(%{port: port}) do
    {:ok, connection} =
      Redix.start_link(
        host: "127.0.0.1",
        port: port,
        password: @password,
        sync_connect: false,
        exit_on_disconnection: false,
        backoff_initial: 10,
        backoff_max: 50
      )

    eventually(fn ->
      Redix.command(connection, ["PING"], timeout: @command_timeout_ms) == {:ok, "PONG"}
    end)

    {StateStoreRedix,
     %{
       connection: connection,
       command_timeout_ms: @command_timeout_ms,
       key_namespace: {"integration-#{System.unique_integer([:positive])}", "v1"}
     }}
  end

  def eventually(fun, timeout_ms \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(25)
        poll(fun, deadline)
      else
        false
      end
    end
  end

  defp port!(harness) do
    harness
    |> command(["port", "valkey", "6379"])
    |> String.trim()
    |> String.split(":")
    |> List.last()
    |> String.to_integer()
  end

  defp run!(harness, args) do
    case command(harness, args) do
      _output -> :ok
    end
  end

  defp command(harness, args) do
    {output, exit_status} =
      System.cmd(
        "docker",
        ["compose", "--project-name", harness.project, "--file", @compose_file | args],
        cd: File.cwd!(),
        env: [{"STATE_STORE_TEST_PASSWORD", @password}],
        stderr_to_stdout: true
      )

    if exit_status == 0, do: output, else: raise("state-store Compose command failed")
  end
end
