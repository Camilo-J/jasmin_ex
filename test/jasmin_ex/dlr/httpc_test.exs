defmodule JasminEx.Dlr.HttpcTest do
  use ExUnit.Case, async: false

  alias JasminEx.Dlr.HttpClient.Httpc
  alias JasminEx.FakeDlrEndpoint

  setup do
    {:ok, endpoint} = FakeDlrEndpoint.start_link(script: [{:reply, 200, "ACK/Jasmin"}])
    on_exit(fn -> FakeDlrEndpoint.stop(endpoint) end)
    %{endpoint: endpoint}
  end

  test "connects to the approved peer exactly once with explicit bounds", %{endpoint: endpoint} do
    context = context(endpoint)

    assert {:ok, 200, "ACK/Jasmin"} =
             Httpc.request(context, %{
               method: "POST",
               url: FakeDlrEndpoint.url(endpoint, "callback.test", "/dlr"),
               headers: [{"content-type", "application/x-www-form-urlencoded"}],
               body: "id=G1"
             })

    assert [%{method: "POST", path: "/dlr", body: "id=G1"}] =
             FakeDlrEndpoint.requests(endpoint)
  end

  test "does not follow redirect targets", %{endpoint: endpoint} do
    {:ok, target} = FakeDlrEndpoint.start_link(script: [{:reply, 200, "target"}])
    on_exit(fn -> FakeDlrEndpoint.stop(target) end)

    FakeDlrEndpoint.script(endpoint, [
      {:redirect, FakeDlrEndpoint.url(target, "callback.test", "/target")}
    ])

    assert {:ok, 302, _body} =
             Httpc.request(context(endpoint), request(endpoint, "/redirect"))

    assert length(FakeDlrEndpoint.requests(endpoint)) == 1
    assert FakeDlrEndpoint.requests(target) == []
  end

  test "enforces total timeout and response body bounds", %{endpoint: endpoint} do
    FakeDlrEndpoint.script(endpoint, [{:slow, 100, 200, "ACK/Jasmin"}])
    assert {:error, _reason} = Httpc.request(context(endpoint, timeout: 20), request(endpoint))
    assert length(FakeDlrEndpoint.requests(endpoint)) == 1

    FakeDlrEndpoint.script(endpoint, [{:reply, 200, String.duplicate("x", 128)}])

    assert {:error, _reason} =
             Httpc.request(context(endpoint, max_body_size: 64), request(endpoint))

    assert length(FakeDlrEndpoint.requests(endpoint)) == 2
  end

  test "pins the HTTPS peer while verifying the original host with SNI" do
    {:ok, endpoint} =
      FakeDlrEndpoint.start_link(
        scheme: :https,
        certificate_host: "callback.test",
        script: [{:reply, 200, "ACK/Jasmin"}]
      )

    on_exit(fn -> FakeDlrEndpoint.stop(endpoint) end)

    assert {:ok, 200, "ACK/Jasmin"} =
             Httpc.request(context(endpoint), request(endpoint))

    mismatch_context =
      context(endpoint,
        resolver: resolver("wrong.test"),
        allow: [{"wrong.test", {127, 0, 0, 1}}]
      )

    mismatch_request = %{request(endpoint) | url: FakeDlrEndpoint.url(endpoint, "wrong.test")}
    assert {:error, _reason} = Httpc.request(mismatch_context, mismatch_request)
    assert length(FakeDlrEndpoint.requests(endpoint)) == 1
  end

  test "one client call never retries a failed network attempt", %{endpoint: endpoint} do
    FakeDlrEndpoint.script(endpoint, [
      {:slow, 100, 200, "ACK/Jasmin"},
      {:reply, 200, "ACK/Jasmin"}
    ])

    assert {:error, _reason} = Httpc.request(context(endpoint, timeout: 20), request(endpoint))
    assert length(FakeDlrEndpoint.requests(endpoint)) == 1
  end

  defp request(endpoint, path \\ "/dlr") do
    %{
      method: "GET",
      url: FakeDlrEndpoint.url(endpoint, "callback.test", path),
      headers: [],
      body: ""
    }
  end

  defp context(endpoint, overrides \\ []) do
    defaults = [
      resolver: resolver("callback.test"),
      allow: [{"callback.test", {127, 0, 0, 1}}],
      profile: String.to_atom("dlr_httpc_#{System.unique_integer([:positive])}"),
      connect_timeout: 100,
      timeout: 500,
      max_header_size: 4_096,
      max_body_size: 65_536,
      cacertfile: FakeDlrEndpoint.certificate(endpoint)
    ]

    Keyword.merge(defaults, overrides)
  end

  defp resolver(host), do: {JasminEx.FakeDlrEndpoint.Resolver, %{host => [{127, 0, 0, 1}]}}
end

defmodule JasminEx.Dlr.HttpThrowerBrokerIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 120_000

  alias JasminEx.Dlr.HttpClient.Httpc
  alias JasminEx.Dlr.{HttpJob, HttpThrower, Worker}
  alias JasminEx.FakeDlrEndpoint
  alias JasminEx.Messaging.RabbitMQ.{Client, Config, TopicTopology}
  alias JasminEx.RabbitMQHarness

  defmodule Clock do
    def now_ms(_context), do: System.system_time(:millisecond)
  end

  test "broker retries one failed callback and ACKs the successful redelivery" do
    harness = RabbitMQHarness.new()
    :ok = RabbitMQHarness.start!(harness)
    on_exit(fn -> safe_stop(harness) end)

    {:ok, endpoint} =
      FakeDlrEndpoint.start_link(script: [{:reply, 500, "failure"}, {:reply, 200, "ACK/Jasmin"}])

    on_exit(fn -> FakeDlrEndpoint.stop(endpoint) end)

    {:ok, conn} = Client.open_connection(connection_opts(harness))
    {:ok, channel} = Client.open_channel(conn)
    on_exit(fn -> Client.close_connection(conn) end)

    prefix = "jasmin_ex.dlr.thrower.#{System.unique_integer([:positive])}"
    :ok = TopicTopology.declare(channel, prefix: prefix, client: Client)
    names = TopicTopology.names(prefix)
    parent = self()

    client_context = [
      resolver: {FakeDlrEndpoint.Resolver, %{"callback.test" => [{127, 0, 0, 1}]}},
      allow: [{"callback.test", {127, 0, 0, 1}}],
      profile: String.to_atom("dlr_broker_httpc_#{System.unique_integer([:positive])}"),
      connect_timeout: 500,
      timeout: 2_000,
      max_header_size: 4_096,
      max_body_size: 65_536
    ]

    processor = fn payload, meta ->
      outcome =
        HttpThrower.process(payload, meta,
          client: {Httpc, client_context},
          clock: {Clock, nil}
        )

      send(parent, {:attempt, delivery_count(meta), outcome})
      outcome
    end

    {:ok, worker} =
      Worker.start_link(
        queue_kind: :http,
        queue: names.http,
        processor: processor,
        client: Client,
        connection: conn,
        name: nil
      )

    on_exit(fn -> if Process.alive?(worker), do: GenServer.stop(worker) end)

    now = System.system_time(:millisecond)

    job = %{
      job_id: "broker-job",
      event_id: "broker-event",
      gateway_id: "G1",
      url: FakeDlrEndpoint.url(endpoint, "callback.test"),
      method: "POST",
      level: 1,
      created_at_ms: now,
      deadline_ms: now + 120_000,
      fields: %{
        "id" => "G1",
        "level" => "1",
        "message_status" => "ESME_ROK",
        "connector" => "c1"
      }
    }

    {:ok, payload} = HttpJob.encode(job)
    :ok = Client.select_confirms(channel)
    :ok = Client.publish(channel, names.exchange, "dlr_thrower.http", payload, persistent: true)
    assert true = Client.wait_for_confirms(channel, 2_000)

    assert_receive {:attempt, 0, :retry}, 5_000
    assert_receive {:attempt, 1, :ok}, 40_000
    assert length(FakeDlrEndpoint.requests(endpoint)) == 2
  end

  defp delivery_count(meta) do
    case Client.header(meta, "x-delivery-count") do
      {:ok, count} -> count
      :error -> 0
    end
  end

  defp connection_opts(harness) do
    env = Map.new(RabbitMQHarness.compose_environment(harness))

    Config.to_connection_options(
      Config.new!(
        host: "127.0.0.1",
        port: RabbitMQHarness.port(harness),
        username: env["RABBITMQ_TEST_USER"],
        password: env["RABBITMQ_TEST_PASSWORD"],
        confirm_timeout_ms: 2_000
      )
    )
  end

  defp safe_stop(harness) do
    RabbitMQHarness.stop!(harness)
  rescue
    _error -> :ok
  end
end
