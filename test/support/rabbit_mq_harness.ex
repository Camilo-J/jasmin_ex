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
      port: available_port!(),
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

  defp safe_stop(harness) do
    stop!(harness)
  rescue
    _cleanup_error -> :ok
  end

  def compose_environment(harness) do
    [
      {"RABBITMQ_TEST_IMAGE", harness.image},
      {"RABBITMQ_TEST_PORT", Integer.to_string(harness.port)},
      {"RABBITMQ_TEST_USER", System.get_env("RABBITMQ_TEST_USER") || "jasmin"},
      {"RABBITMQ_TEST_PASSWORD",
       System.get_env("RABBITMQ_TEST_PASSWORD") || "jasmin-test-password"}
    ]
  end

  defp validate_image!(@allowed_image), do: :ok

  defp validate_image!(_image) do
    raise ArgumentError, "RabbitMQ image is not allowlisted"
  end

  defp available_port! do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp run!(harness, args) do
    {_output, exit_status} = command(harness, args)

    if exit_status == 0 do
      :ok
    else
      raise "RabbitMQ Compose command failed"
    end
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
