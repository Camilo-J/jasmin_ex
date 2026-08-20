defmodule JasminEx.Messaging.RabbitMQ.MeasurementsTest do
  use ExUnit.Case, async: true

  alias JasminEx.RabbitMQMeasurements

  @required [
    :connector_count,
    :rate,
    :payload,
    :backlog,
    :latency,
    :cpu,
    :memory,
    :alarm,
    :confirm,
    :redelivery,
    :recovery
  ]

  test "an empty baseline is incomplete validation and forbids any fitness claim" do
    report = RabbitMQMeasurements.evaluate(%{})

    assert report.validation == :incomplete
    assert report.fitness_claim == false
    assert report.missing == @required
  end

  test "a single missing metric stays incomplete and still forbids fitness" do
    report = RabbitMQMeasurements.evaluate(Map.delete(complete_baseline(), :cpu))

    assert report.validation == :incomplete
    assert report.fitness_claim == false
    assert report.missing == [:cpu]
  end

  test "a different missing metric reports that gap and still forbids fitness" do
    report = RabbitMQMeasurements.evaluate(Map.delete(complete_baseline(), :recovery))

    assert report.validation == :incomplete
    assert report.fitness_claim == false
    assert report.missing == [:recovery]
  end

  test "durable restart recording retains a complete baseline without a fitness claim" do
    run = %{path: :durable_restart, broker: :pinned}
    recorded = RabbitMQMeasurements.record(run, complete_baseline())

    assert recorded.run == run
    assert recorded.baseline == complete_baseline()
    assert recorded.validation == :complete
    assert recorded.fitness_claim == false
    refute Map.has_key?(recorded, :instance_type)
    refute Map.has_key?(recorded.baseline, :threshold)
    refute inspect(recorded) =~ "t2.micro"
  end

  test "durable restart recording with a gap stays incomplete and forbids fitness" do
    run = %{path: :durable_restart, broker: :pinned}
    recorded = RabbitMQMeasurements.record(run, Map.delete(complete_baseline(), :backlog))

    assert recorded.run == run
    assert recorded.validation == :incomplete
    assert recorded.fitness_claim == false
    assert recorded.missing == [:backlog]
  end

  test "README documents enablement, topology, quarantine ownership, rollback, and evidence" do
    readme = File.read!("README.md")

    assert readme =~ "enabled: false"
    assert readme =~ "jasmin.work.<connector_id>"
    assert readme =~ "jasmin.work.<connector_id>.quarantine"
    assert readme =~ "Operations owns"
    assert readme =~ "no TTL"
    assert readme =~ "replay"
    assert readme =~ "automatic purge"
    assert readme =~ "Disable publish and consume"
    assert readme =~ "Drain or quarantine"
    assert readme =~ "Keep queues and the evidence journal"
    assert readme =~ "mix test --include integration"
    assert readme =~ "not broker proof"
    assert readme =~ "incomplete validation"
  end

  test "README keeps state-store docs and adds a one-line RabbitMQ compose boundary" do
    readme = File.read!("README.md")

    assert readme =~ "State-store integration evidence"
    assert readme =~ "redis:8.0.3-bookworm@"
    assert readme =~ "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.3@"
    assert readme =~ "must not start Valkey, Redis, or Dragonfly"
  end

  test "test helper loads the measurement helper" do
    helper = File.read!("test/test_helper.exs")
    assert helper =~ ~s[Code.require_file("test/support/rabbit_mq_measurements.ex")]
  end

  defp complete_baseline do
    Map.new(@required, fn
      :confirm -> {:confirm, :ok}
      :recovery -> {:recovery, :passed}
      key -> {key, 1}
    end)
  end
end
