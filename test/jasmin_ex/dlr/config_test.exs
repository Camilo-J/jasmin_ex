defmodule JasminEx.Dlr.ConfigTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Config

  test "defaults match the Python-compatible retry and expiry contract" do
    assert %Config{} = config = Config.new()
    assert config.enabled == false
    assert config.queue_prefix == "jasmin_ex.dlr"
    assert config.lookup_additional_attempts == 2
    assert config.lookup_delay_ms == 10_000
    assert config.http_additional_attempts == 3
    assert config.http_delay_ms == 30_000
    assert config.http_timeout_ms == 30_000
    assert config.dlr_expiry_s == 86_400
    assert Config.connector_expiry(config) == 86_400
    assert Config.connector_expiry(config, nil) == 86_400
    assert Config.connector_expiry(config, 3_600) == 3_600
  end

  test "invalid configuration is rejected" do
    assert {:error, :invalid_dlr_config} = Config.new(enabled: :yes)
    assert {:error, :invalid_dlr_config} = Config.new(lookup_additional_attempts: 0)
    assert {:error, :invalid_dlr_config} = Config.new(http_additional_attempts: -1)
    assert {:error, :invalid_dlr_config} = Config.new(dlr_expiry_s: 0)
    assert {:error, :invalid_dlr_config} = Config.new(queue_prefix: "")
    assert {:error, :invalid_dlr_config} = Config.connector_expiry(Config.new(), 0)
  end
end
