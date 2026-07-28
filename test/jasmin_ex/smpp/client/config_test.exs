defmodule JasminEx.Smpp.Client.ConfigTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.Client.Config
  alias JasminEx.Smpp.Client.ReconnectPolicy

  @required_opts [
    host: ~c"localhost",
    port: 2775,
    system_id: "system-id",
    password: "password",
    system_type: "system-type",
    bind_as: :transceiver
  ]

  test "normalizes the complete default configuration" do
    assert %Config{
             host: ~c"localhost",
             port: 2775,
             system_id: "system-id",
             password: "password",
             system_type: "system-type",
             bind_as: :transceiver,
             heartbeat_ms: 30_000,
             response_timeout_ms: 5_000,
             unbind_drain_timeout_ms: 5_000,
             reconnect: %ReconnectPolicy{
               base_ms: 1_000,
               factor: 2,
               cap_ms: 30_000,
               jitter: true
             },
             deliver_handler: {nil, nil}
           } = Config.new!(@required_opts)
  end

  test "normalizes explicit values and deliver handler context" do
    opts =
      Keyword.merge(@required_opts,
        heartbeat_ms: 10,
        response_timeout_ms: 20,
        unbind_drain_timeout_ms: 30,
        reconnect_base_ms: 40,
        reconnect_factor: 3,
        reconnect_cap_ms: 50,
        reconnect_jitter: false,
        deliver_handler: {__MODULE__, :context}
      )

    assert %Config{
             heartbeat_ms: 10,
             response_timeout_ms: 20,
             unbind_drain_timeout_ms: 30,
             reconnect: %ReconnectPolicy{
               base_ms: 40,
               factor: 3,
               cap_ms: 50,
               jitter: false
             },
             deliver_handler: {__MODULE__, :context}
           } = Config.new!(opts)
  end

  test "retains first-keyword-wins option semantics" do
    opts =
      [
        host: ~c"first",
        host: ~c"second",
        heartbeat_ms: 5,
        heartbeat_ms: 6,
        response_timeout_ms: 10,
        response_timeout_ms: 20,
        unbind_drain_timeout_ms: 15,
        unbind_drain_timeout_ms: 25,
        reconnect_base_ms: 30,
        reconnect_base_ms: 40,
        deliver_handler: {__MODULE__, :first},
        deliver_handler: {__MODULE__, :second}
      ] ++ Keyword.delete(@required_opts, :host)

    config = Config.new!(opts)

    assert config.host == ~c"first"
    assert config.heartbeat_ms == 5
    assert config.response_timeout_ms == 10
    assert config.unbind_drain_timeout_ms == 15
    assert config.reconnect.base_ms == 30
    assert config.deliver_handler == {__MODULE__, :first}
  end

  test "raises for every missing required key" do
    for key <- [:host, :port, :system_id, :password, :system_type, :bind_as] do
      assert_raise KeyError, ~r/key #{inspect(key)} not found/, fn ->
        @required_opts |> Keyword.delete(key) |> Config.new!()
      end
    end
  end

  test "preserves unbind drain timeout validation and error messages" do
    for timeout <- [-1, 1.5] do
      message =
        ":unbind_drain_timeout_ms must be a non-negative integer, got: #{inspect(timeout)}"

      assert_raise ArgumentError, message, fn ->
        Config.new!(Keyword.put(@required_opts, :unbind_drain_timeout_ms, timeout))
      end
    end
  end

  test "validates the effective drain timeout before fetching required keys" do
    assert_raise ArgumentError,
                 ":unbind_drain_timeout_ms must be a non-negative integer, got: -1",
                 fn -> Config.new!(unbind_drain_timeout_ms: -1) end
  end

  test "preserves malformed deliver handler failure behavior" do
    assert_raise FunctionClauseError, fn ->
      Config.new!(Keyword.put(@required_opts, :deliver_handler, __MODULE__))
    end
  end
end
