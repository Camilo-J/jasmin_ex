defmodule JasminEx.Messaging.RabbitMQ.CIEvidenceTest do
  use ExUnit.Case, async: true

  alias JasminEx.RabbitMQHarness

  @workflow_path ".github/workflows/ci.yml"
  @compose_path "compose.rabbitmq.yml"
  @job_name "rabbitmq-4-3-4"

  @integration_command "mix test --include integration test/jasmin_ex/messaging/rabbit_mq/integration_test.exs"

  test "defines pinned RabbitMQ 4.3.4 durable/restart evidence as a dedicated CI job" do
    workflow = File.read!(@workflow_path)
    job = job_body(workflow, @job_name)

    assert job =~ "RABBITMQ_TEST_IMAGE: #{RabbitMQHarness.allowed_image()}"
    assert job =~ @integration_command
    refute job =~ "continue-on-error:"
    refute job =~ "t2.micro"

    assert github_honored_pipeline_failure?(evidence_run_step(job)),
           "mix test | tee must fail the job when mix test fails; GitHub's default bash -e {0} reports tee's status"
  end

  test "absence of continue-on-error does not prove mix test failure propagates through tee" do
    tee_pipeline = """
          run: #{@integration_command} 2>&1 | tee rabbitmq-4-3-4.txt
    """

    refute github_honored_pipeline_failure?(tee_pipeline)

    assert github_honored_pipeline_failure?("""
             shell: bash
             #{tee_pipeline}
           """)

    assert github_honored_pipeline_failure?("""
             run: |
               set -o pipefail
               #{@integration_command} 2>&1 | tee rabbitmq-4-3-4.txt
           """)

    assert github_honored_pipeline_failure?("""
             run: |
               #{@integration_command} 2>&1 | tee rabbitmq-4-3-4.txt
               exit ${PIPESTATUS[0]}
           """)
  end

  test "uploads a RabbitMQ evidence artifact even when the job fails" do
    workflow = File.read!(@workflow_path)
    job = job_body(workflow, @job_name)

    assert job =~ "actions/upload-artifact@"
    assert job =~ "if: ${{ always() }}"
    assert job =~ "name: rabbitmq-4-3-4"
    assert job =~ "path: rabbitmq-4-3-4.txt"
    assert job =~ "if-no-files-found: error"
    assert job =~ "tee rabbitmq-4-3-4.txt"
  end

  test "RabbitMQ CI evidence does not start Valkey, Redis, or Dragonfly" do
    workflow = File.read!(@workflow_path)
    job = job_body(workflow, @job_name)

    refute job =~ "STATE_STORE_TEST_IMAGE"
    refute job =~ "compose.state-store.yml"
    refute job =~ "valkey"
    refute job =~ "redis"
    refute job =~ "dragonfly"
    refute job =~ "state-store"
  end

  test "blocking ci job keeps Valkey evidence and does not start RabbitMQ" do
    workflow = File.read!(@workflow_path)
    job = job_body(workflow, "ci")

    assert job =~
             "mix test --include integration test/jasmin_ex/state_store/integration_test.exs"

    refute job =~ "rabbit_mq/integration_test.exs"
    refute job =~ "RABBITMQ_TEST_IMAGE"
    refute job =~ "compose.rabbitmq.yml"
  end

  test "RabbitMQ compose file stays on the broker side of the store boundary" do
    compose = File.read!(@compose_path)

    assert compose =~ "services:"
    assert compose =~ ~r/^  rabbitmq:/m
    refute compose =~ "valkey"
    refute compose =~ "redis"
    refute compose =~ "dragonfly"
    refute compose =~ "compose.state-store.yml"
  end

  test "README documents CI evidence without a fitness claim or shared store compose" do
    readme = File.read!("README.md")

    assert readme =~ @job_name
    assert readme =~ "must not start Valkey, Redis, or Dragonfly"
    assert readme =~ ~r/not an environment fitness claim/i
  end

  defp evidence_run_step(job) do
    pattern = ~r/^      - name: Run RabbitMQ durable\/restart evidence\n(?:        .*\n)*/m

    case Regex.run(pattern, job) do
      [step] -> step
      nil -> flunk("missing RabbitMQ evidence run step")
    end
  end

  defp github_honored_pipeline_failure?(step) do
    String.contains?(step, "| tee rabbitmq-4-3-4.txt") and pipefail_or_equivalent?(step)
  end

  defp pipefail_or_equivalent?(step) do
    # GitHub's unspecified Linux shell is `bash -e {0}` (no pipefail).
    # `shell: bash` uses `bash --noprofile --norc -eo pipefail {0}`.
    Regex.match?(~r/^\s+shell:\s+bash\s*$/m, step) or
      Regex.match?(~r/shell:.*pipefail/, step) or
      String.contains?(step, "set -o pipefail") or
      String.contains?(step, "PIPESTATUS[0]")
  end

  defp job_body(workflow, job_name) do
    pattern = ~r/^  #{Regex.escape(job_name)}:\n(?:    .*\n|\n)*/m

    case Regex.run(pattern, workflow) do
      [body] -> body
      nil -> flunk("missing CI job #{job_name}")
    end
  end
end
