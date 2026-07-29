defmodule JasminEx.StateStore.ConfigTest do
  use ExUnit.Case, async: true

  alias JasminEx.StateStore.Config

  test "normalizes endpoint, DB 0, authentication, TLS, and bounded policy" do
    config =
      Config.new!(
        endpoint: "rediss://cache.example:6380",
        username: "app",
        password: "secret",
        prefix: "deployment",
        version: "v2",
        connect_timeout_ms: 10,
        command_timeout_ms: 20,
        health_check_timeout_ms: 30,
        backoff_initial_ms: 40,
        backoff_max_ms: 50
      )

    assert %{
             host: "cache.example",
             port: 6380,
             database: 0,
             tls: true,
             username: "app",
             password: "secret"
           } = config

    assert config.key_namespace == {"deployment", "v2"}
    assert config.backoff_initial_ms == 40 and config.backoff_max_ms == 50
  end

  test "rejects nonzero DB and userinfo without disclosing credentials" do
    assert_raise ArgumentError, ~r/database must be 0/, fn ->
      Config.new!(endpoint: "redis://cache/1")
    end

    error =
      assert_raise ArgumentError, ~r/endpoint userinfo is not allowed/, fn ->
        Config.new!(endpoint: "redis://user:secret@cache")
      end

    refute Exception.message(error) =~ "secret"
    refute inspect(Config.new!(host: "cache", password: "secret")) =~ "secret"
  end
end
