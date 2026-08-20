defmodule JasminEx.RabbitMQMeasurements do
  @moduledoc false

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

  def evaluate(baseline) when is_map(baseline) do
    missing = Enum.reject(@required, &Map.has_key?(baseline, &1))

    %{
      validation: if(missing == [], do: :complete, else: :incomplete),
      fitness_claim: false,
      missing: missing
    }
  end

  def record(run, observations) when is_map(run) and is_map(observations) do
    Map.merge(evaluate(observations), %{run: run, baseline: observations})
  end
end
