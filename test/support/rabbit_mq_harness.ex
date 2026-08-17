defmodule JasminEx.RabbitMQHarness do
  @moduledoc false

  @compose_file "compose.rabbitmq.yml"
  @allowed_image "rabbitmq:4.3.4@sha256:4b336f82e93749f1ebf8d6283b4d5a98bf1efac8412ec56015a6ab5aae0f57a2"

  def allowed_image, do: @allowed_image

  def new(opts \\ []) do
    image = Keyword.get(opts, :image, System.get_env("RABBITMQ_TEST_IMAGE", @allowed_image))
    validate_image!(image)

    %{
      project: "jasmin-ex-rabbitmq-#{System.unique_integer([:positive])}",
      port: Keyword.get(opts, :port, 0),
      image: image,
      cmd: Keyword.get(opts, :cmd, &System.cmd/3)
    }
  end

  def start!(harness) do
    run!(harness, ["up", "--wait"])
  rescue
    error ->
      _ = safe_stop(harness)
      reraise error, __STACKTRACE__
  end

  def stop!(harness), do: run!(harness, ["down", "--volumes", "--remove-orphans"])

  def restart!(harness) do
    run!(harness, ["restart"])
    wait!(harness)
  end

  def wait!(harness), do: run!(harness, ["up", "--wait"])

  def port(harness) do
    case harness.port do
      port when is_integer(port) and port > 0 -> port
      _ -> discover_port!(harness)
    end
  end

  def compose_environment(harness) do
    [
      {"RABBITMQ_TEST_IMAGE", harness.image},
      {"RABBITMQ_TEST_PORT", Integer.to_string(harness.port || 0)},
      {"RABBITMQ_TEST_USER", System.get_env("RABBITMQ_TEST_USER") || "jasmin"},
      {"RABBITMQ_TEST_PASSWORD",
       System.get_env("RABBITMQ_TEST_PASSWORD") || "jasmin-test-password"}
    ]
  end

  defp safe_stop(harness) do
    stop!(harness)
  rescue
    _cleanup_error -> :ok
  end

  defp validate_image!(@allowed_image), do: :ok

  defp validate_image!(_image) do
    raise ArgumentError, "RabbitMQ image is not allowlisted"
  end

  defp discover_port!(harness) do
    {output, exit_status} = command(harness, ["port", "rabbitmq", "5672"])

    if exit_status != 0 do
      raise compose_error(exit_status, output)
    end

    case Regex.run(~r/:(\d+)\s*$/m, String.trim(output)) do
      [_, port] -> String.to_integer(port)
      _ -> raise compose_error(exit_status, output)
    end
  end

  defp run!(harness, args) do
    {output, exit_status} = command(harness, args)

    if exit_status == 0 do
      :ok
    else
      raise compose_error(exit_status, output)
    end
  end

  defp compose_error(exit_status, output) do
    "RabbitMQ Compose command failed (exit #{exit_status}): #{String.trim(output)}"
  end

  defp command(harness, args) do
    harness.cmd.(
      "docker",
      ["compose", "--project-name", harness.project, "--file", @compose_file | args],
      cd: File.cwd!(),
      env: compose_environment(harness),
      stderr_to_stdout: true
    )
  end
end
