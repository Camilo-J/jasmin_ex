defmodule JasminEx.Messaging.RabbitMQ.HarnessTest do
  use ExUnit.Case, async: true

  alias JasminEx.RabbitMQHarness

  test "rejects an unknown RabbitMQ image" do
    assert_raise ArgumentError, ~r/allowlisted/, fn ->
      RabbitMQHarness.new(image: "rabbitmq:latest")
    end
  end

  test "rejects a non-allowlisted RabbitMQ digest" do
    assert_raise ArgumentError, ~r/allowlisted/, fn ->
      RabbitMQHarness.new(
        image:
          "rabbitmq:4.3.4@sha256:0000000000000000000000000000000000000000000000000000000000000000"
      )
    end
  end

  test "invokes docker compose with a unique project, pinned file, and repo cwd" do
    parent = self()

    cmd = fn executable, args, opts ->
      send(parent, {:cmd, executable, args, opts[:cd]})
      {"", 0}
    end

    first = RabbitMQHarness.new(cmd: cmd)
    second = RabbitMQHarness.new(cmd: cmd)

    assert first.project != second.project
    assert first.project =~ ~r/^jasmin-ex-rabbitmq-\d+$/

    assert :ok = RabbitMQHarness.start!(first)

    assert_received {:cmd, "docker", args, cwd}
    assert cwd == File.cwd!()

    assert args == [
             "compose",
             "--project-name",
             first.project,
             "--file",
             "compose.rabbitmq.yml",
             "up",
             "--wait"
           ]
  end

  test "surfaces a nonzero compose exit and still runs scoped cleanup" do
    parent = self()

    cmd = fn executable, args, opts ->
      send(parent, {:cmd, executable, args, opts[:cd]})

      if "up" in args do
        {"compose failed", 17}
      else
        {"", 0}
      end
    end

    harness = RabbitMQHarness.new(cmd: cmd)

    assert_raise RuntimeError, ~r/Compose command failed/, fn ->
      RabbitMQHarness.start!(harness)
    end

    cwd = File.cwd!()
    assert_received {:cmd, "docker", up_args, ^cwd}

    assert up_args == [
             "compose",
             "--project-name",
             harness.project,
             "--file",
             "compose.rabbitmq.yml",
             "up",
             "--wait"
           ]

    assert_received {:cmd, "docker", down_args, ^cwd}

    assert down_args == [
             "compose",
             "--project-name",
             harness.project,
             "--file",
             "compose.rabbitmq.yml",
             "down",
             "--volumes",
             "--remove-orphans"
           ]
  end

  test "pins the official RabbitMQ 4.3.4 digest and binds the published port to loopback" do
    compose = File.read!("compose.rabbitmq.yml")

    assert compose =~ RabbitMQHarness.allowed_image()
    assert compose =~ "host_ip: 127.0.0.1"
    assert compose =~ "healthcheck:"
    assert compose =~ "rabbitmq-diagnostics"
    refute compose =~ "://"
  end

  test "passes credentials and the selected port through environment variables" do
    harness = RabbitMQHarness.new()
    env = RabbitMQHarness.compose_environment(harness)

    assert {"RABBITMQ_TEST_IMAGE", RabbitMQHarness.allowed_image()} in env
    assert {"RABBITMQ_TEST_PORT", Integer.to_string(harness.port)} in env
    assert List.keymember?(env, "RABBITMQ_TEST_USER", 0)
    assert List.keymember?(env, "RABBITMQ_TEST_PASSWORD", 0)

    refute Enum.any?(env, fn {_key, value} ->
             String.contains?(value, "://") and String.contains?(value, "@")
           end)
  end

  test "test helper loads the harness and keeps ordinary tests off Docker" do
    helper = File.read!("test/test_helper.exs")

    assert helper =~ ~s[Code.require_file("test/support/rabbit_mq_harness.ex")]
    assert helper =~ "exclude: [:compatibility, :integration]"
  end
end
