defmodule JasminEx.StateStoreHarness do
  @moduledoc false

  alias JasminEx.StateStore.Redix, as: StateStoreRedix

  @compose_file "compose.state-store.yml"
  @password "state-store-test-password"
  @command_timeout_ms 250
  @valkey_image "valkey/valkey:9.1.1@sha256:2f4a4b0a42a72569b40567fae9016dc54aa76736250be28120b5fced8050c0f0"
  @redis_image "redis:8.0.3-bookworm@sha256:be0a135f1955140436b9114da96dd22fbedb874469400b6ef458cc0d42155de0"
  @dragonfly_image "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.3@sha256:29d44a25a9e6937672f1c12e28c9f481f3d3c0441001ee56ed274a72f50593b7"

  def new do
    image = System.get_env("STATE_STORE_TEST_IMAGE", @valkey_image)

    %{
      project: "jasmin-ex-state-store-#{System.unique_integer([:positive])}",
      port: available_port!(),
      image: image
    }
  end

  def start! do
    harness = new()

    try do
      :ok = start_valkey!(harness)
      harness
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

  def compose_environment(harness) do
    [
      {"STATE_STORE_TEST_PASSWORD", @password},
      {"STATE_STORE_TEST_PORT", Integer.to_string(harness.port)},
      {"STATE_STORE_TEST_IMAGE", harness.image},
      {"STATE_STORE_TEST_COMMAND", server_command(harness.image)}
    ]
  end

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

  defp available_port! do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp server_command(@dragonfly_image), do: "dragonfly --logtostderr --requirepass #{@password}"
  defp server_command(@redis_image), do: "redis-server --requirepass #{@password}"
  defp server_command(_image), do: "valkey-server --requirepass #{@password}"

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
        env: compose_environment(harness),
        stderr_to_stdout: true
      )

    if exit_status == 0, do: output, else: raise("state-store Compose command failed")
  end
end
