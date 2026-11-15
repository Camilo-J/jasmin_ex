defmodule JasminEx.Dlr.ConfigTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Config

  test "defaults match the retry contract and the one-hour delivery windows" do
    assert %Config{} = config = Config.new()
    assert config.enabled == false
    assert config.queue_prefix == "jasmin_ex.dlr"
    assert config.lookup_additional_attempts == 2
    assert config.lookup_delay_ms == 10_000
    assert config.http_additional_attempts == 3
    assert config.http_delay_ms == 30_000
    assert config.http_timeout_ms == 30_000
    assert config.dlr_deadline_s == 3_600
    assert config.dlr_closed_retention_s == 3_600
    refute Map.has_key?(config, :dlr_expiry_s)
  end

  test "connectors without overrides use the global windows" do
    config = Config.new(dlr_deadline_s: 600, dlr_closed_retention_s: 300)

    assert Config.windows(config, "unknown") == %{deadline_s: 600, closed_retention_s: 300}
    assert Config.lifetime_s(config, "unknown") == 900
  end

  test "an explicit connector value wins over the global value" do
    assert %Config{} =
             config =
             Config.new(dlr_deadline_s: 600, dlr_closed_retention_s: 300)
             |> Config.with_connectors([
               [connector_id: "c1", dlr_deadline: 120],
               %{connector_id: "c2", dlr_closed_retention: 60},
               [connector_id: "c3"]
             ])

    assert Config.windows(config, "c1") == %{deadline_s: 120, closed_retention_s: 300}
    assert Config.windows(config, "c2") == %{deadline_s: 600, closed_retention_s: 60}
    assert Config.windows(config, "c3") == %{deadline_s: 600, closed_retention_s: 300}
    assert Config.lifetime_s(config, "c1") == 420
  end

  test "connectors declare an optional correlation group" do
    config =
      Config.with_connectors(Config.new(), [
        [connector_id: "c1", dlr_correlation_group: "smsc-x"],
        [connector_id: "c2"]
      ])

    assert Config.correlation_group(config, "c1") == "smsc-x"
    assert Config.correlation_group(config, "c2") == "c2"
    assert Config.correlation_group(config, "unknown") == "unknown"

    assert {:error, :invalid_dlr_config} =
             Config.with_connectors(Config.new(), [
               [connector_id: "c1", dlr_correlation_group: ""]
             ])
  end

  test "a single keyword connector is accepted" do
    config = Config.with_connectors(Config.new(), connector_id: "c1", dlr_deadline: 30)

    assert Config.windows(config, "c1") == %{deadline_s: 30, closed_retention_s: 3_600}
  end

  test "invalid configuration is rejected" do
    assert {:error, :invalid_dlr_config} = Config.new(enabled: :yes)
    assert {:error, :invalid_dlr_config} = Config.new(lookup_additional_attempts: 0)
    assert {:error, :invalid_dlr_config} = Config.new(http_additional_attempts: -1)
    assert {:error, :invalid_dlr_config} = Config.new(dlr_deadline_s: 0)
    assert {:error, :invalid_dlr_config} = Config.new(dlr_closed_retention_s: -1)
    assert {:error, :invalid_dlr_config} = Config.new(queue_prefix: "")
  end

  test "invalid connector windows are rejected" do
    for connector <- [
          [connector_id: "c1", dlr_deadline: 0],
          [connector_id: "c1", dlr_closed_retention: "60"]
        ] do
      assert {:error, :invalid_dlr_config} = Config.with_connectors(Config.new(), [connector])
    end
  end
end
