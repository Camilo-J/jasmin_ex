defmodule JasminEx.Messaging.RabbitMQ.ConfigTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.Config

  @valid_opts [
    endpoint: "amqps://broker.example:5671",
    username: "app",
    password: "super-secret",
    virtual_host: "jasmin",
    queue_prefix: "jasmin.work",
    confirm_timeout_ms: 5_000,
    max_attempts: 3,
    default_expiry_ms: 86_400_000,
    quarantine_depth_alarm: 100,
    quarantine_age_ms_alarm: 3_600_000
  ]

  test "normalizes a valid AMQPS endpoint with TLS, bounds, and alarms" do
    config = Config.new!(@valid_opts)

    assert %Config{
             host: "broker.example",
             port: 5671,
             tls: true,
             username: "app",
             password: "super-secret",
             virtual_host: "jasmin",
             queue_prefix: "jasmin.work",
             confirm_timeout_ms: 5_000,
             max_attempts: 3,
             default_expiry_ms: 86_400_000,
             quarantine_depth_alarm: 100,
             quarantine_age_ms_alarm: 3_600_000
           } = config

    assert Keyword.keyword?(config.ssl_options)
    assert config.ssl_options[:verify] == :verify_peer
  end

  test "defaults AMQP endpoint port, virtual host, queue prefix, and policy bounds" do
    config =
      Config.new!(
        endpoint: "amqp://broker.local",
        username: "guest",
        password: "guest"
      )

    assert config.host == "broker.local"
    assert config.port == 5672
    assert config.tls == false
    assert config.ssl_options == :none
    assert config.virtual_host == "/"
    assert config.queue_prefix == "jasmin.work"
    assert config.confirm_timeout_ms == 5_000
    assert config.max_attempts == 3
    assert config.default_expiry_ms == 86_400_000
    assert config.quarantine_depth_alarm == 1_000
    assert config.quarantine_age_ms_alarm == 86_400_000
  end

  test "accepts discrete host/port options and explicit TLS enablement on AMQP" do
    config =
      Config.new!(
        host: "mq.internal",
        port: 5673,
        tls: true,
        username: "svc",
        password: "token",
        ssl_options: [verify: :verify_peer, cacertfile: ~c"/etc/ssl/certs/ca.pem"]
      )

    assert config.host == "mq.internal"
    assert config.port == 5673
    assert config.tls == true
    assert config.ssl_options[:cacertfile] == ~c"/etc/ssl/certs/ca.pem"
  end

  test "defaults the discrete host port from TLS while preserving explicit ports" do
    base_opts = [host: "mq.internal", username: "svc", password: "token"]

    assert Config.new!(base_opts).port == 5672
    assert Config.new!(Keyword.put(base_opts, :tls, true)).port == 5671

    assert Config.new!(Keyword.merge(base_opts, tls: true, port: 5673)).port == 5673
  end

  test "derives the virtual host from a percent-encoded endpoint path" do
    config =
      Config.new!(
        endpoint: "amqp://broker.example/%2Fjasmin%2Fproduction",
        username: "app",
        password: "secret"
      )

    assert config.virtual_host == "/jasmin/production"
  end

  test "accepts a matching explicit virtual host for an endpoint path" do
    config =
      Config.new!(
        endpoint: "amqp://broker.example/jasmin",
        username: "app",
        password: "secret",
        virtual_host: "jasmin"
      )

    assert config.virtual_host == "jasmin"
  end

  test "preserves the default virtual host for root and absent endpoint paths" do
    base_opts = [username: "app", password: "secret"]

    assert Config.new!(Keyword.put(base_opts, :endpoint, "amqp://broker.example")).virtual_host ==
             "/"

    assert Config.new!(Keyword.put(base_opts, :endpoint, "amqp://broker.example/")).virtual_host ==
             "/"
  end

  test "rejects a conflicting endpoint path and explicit virtual host" do
    assert_raise ArgumentError, ~r/virtual_host conflicts with endpoint path/, fn ->
      Config.new!(
        endpoint: "amqp://broker.example/jasmin",
        username: "app",
        password: "secret",
        virtual_host: "other"
      )
    end
  end

  test "rejects invalid endpoint path encoding without disclosing credentials" do
    error =
      assert_raise ArgumentError, ~r/endpoint path has invalid percent-encoding/, fn ->
        Config.new!(
          endpoint: "amqp://broker.example/%invalid",
          username: "app",
          password: "super-secret"
        )
      end

    refute Exception.message(error) =~ "super-secret"
  end

  test "rejects invalid endpoint scheme without disclosing credentials" do
    error =
      assert_raise ArgumentError, ~r/endpoint scheme is invalid/, fn ->
        Config.new!(endpoint: "redis://broker.example", username: "app", password: "super-secret")
      end

    refute Exception.message(error) =~ "super-secret"
  end

  test "rejects missing endpoint host" do
    assert_raise ArgumentError, ~r/endpoint host is required/, fn ->
      Config.new!(endpoint: "amqp://", username: "app", password: "secret")
    end
  end

  test "rejects invalid endpoint port" do
    assert_raise ArgumentError, ~r/endpoint port is invalid/, fn ->
      Config.new!(endpoint: "amqp://broker.example:0", username: "app", password: "secret")
    end

    assert_raise ArgumentError, ~r/endpoint port is invalid/, fn ->
      Config.new!(host: "broker.example", port: 70_000, username: "app", password: "secret")
    end
  end

  test "rejects endpoint userinfo and keeps credentials out of errors" do
    error =
      assert_raise ArgumentError, ~r/endpoint userinfo is not allowed/, fn ->
        Config.new!(endpoint: "amqp://user:super-secret@broker.example")
      end

    refute Exception.message(error) =~ "super-secret"
  end

  test "rejects invalid TLS option shapes" do
    assert_raise ArgumentError, ~r/tls must be a boolean/, fn ->
      Config.new!(Keyword.put(@valid_opts, :tls, "yes"))
    end

    assert_raise ArgumentError, ~r/ssl_options must be a keyword list or :none/, fn ->
      Config.new!(Keyword.put(@valid_opts, :ssl_options, %{verify: :verify_peer}))
    end

    assert_raise ArgumentError, ~r/ssl_options require tls/, fn ->
      Config.new!(
        endpoint: "amqp://broker.example",
        username: "app",
        password: "secret",
        ssl_options: [verify: :verify_peer]
      )
    end
  end

  test "rejects unsafe queue prefixes" do
    for prefix <- ["", "jasmin work", "jasmin/#", "jasmin.*", "/absolute", "ends.", "-leading"] do
      assert_raise ArgumentError, ~r/queue_prefix is invalid/, fn ->
        Config.new!(Keyword.put(@valid_opts, :queue_prefix, prefix))
      end
    end
  end

  test "rejects non-positive confirm timeout" do
    for timeout <- [0, -1, 1.5, "5000"] do
      assert_raise ArgumentError, ~r/confirm_timeout_ms must be a positive integer/, fn ->
        Config.new!(Keyword.put(@valid_opts, :confirm_timeout_ms, timeout))
      end
    end
  end

  test "rejects invalid retry and expiry bounds" do
    assert_raise ArgumentError, ~r/max_attempts must be a positive integer/, fn ->
      Config.new!(Keyword.put(@valid_opts, :max_attempts, 0))
    end

    assert_raise ArgumentError, ~r/default_expiry_ms must be a positive integer/, fn ->
      Config.new!(Keyword.put(@valid_opts, :default_expiry_ms, -5))
    end
  end

  test "rejects invalid alarm thresholds" do
    assert_raise ArgumentError, ~r/quarantine_depth_alarm must be a positive integer/, fn ->
      Config.new!(Keyword.put(@valid_opts, :quarantine_depth_alarm, 0))
    end

    assert_raise ArgumentError, ~r/quarantine_age_ms_alarm must be a positive integer/, fn ->
      Config.new!(Keyword.put(@valid_opts, :quarantine_age_ms_alarm, -1))
    end
  end

  test "inspect redacts credentials while connection options remain usable" do
    config = Config.new!(@valid_opts)
    inspected = inspect(config)

    refute inspected =~ "super-secret"
    refute inspected =~ "password:"

    options = Config.to_connection_options(config)

    assert options[:host] == ~c"broker.example"
    assert options[:port] == 5671
    assert options[:username] == "app"
    assert options[:password] == "super-secret"
    assert options[:virtual_host] == "jasmin"
    assert is_list(options[:ssl_options])
  end

  test "requires username and password" do
    assert_raise ArgumentError, ~r/username is required/, fn ->
      Config.new!(endpoint: "amqp://broker.example", password: "secret")
    end

    assert_raise ArgumentError, ~r/password is required/, fn ->
      Config.new!(endpoint: "amqp://broker.example", username: "app")
    end
  end
end
