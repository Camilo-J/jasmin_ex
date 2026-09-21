defmodule JasminEx.Dlr.MintTest do
  use ExUnit.Case, async: false

  alias JasminEx.Dlr.HttpClient.Mint, as: MintClient
  alias JasminEx.FakeDlrEndpoint

  defmodule RecordingResolver do
    @moduledoc false

    def resolve({owner, address}, host) do
      send(owner, {:resolved, host})
      {:ok, [address]}
    end
  end

  defmodule RawEndpoint do
    @moduledoc false

    def start(script, owner) do
      {:ok, listener} =
        :gen_tcp.listen(0, [
          :binary,
          active: false,
          packet: :raw,
          ip: {127, 0, 0, 1},
          reuseaddr: true
        ])

      {:ok, {_address, port}} = :inet.sockname(listener)
      pid = spawn(fn -> serve(listener, script, owner) end)
      %{listener: listener, pid: pid, port: port}
    end

    def stop(endpoint) do
      :gen_tcp.close(endpoint.listener)
      Process.exit(endpoint.pid, :normal)
      :ok
    end

    def url(endpoint, path \\ "/dlr"),
      do: "http://callback.test:#{endpoint.port}#{path}"

    defp serve(listener, script, owner) do
      with {:ok, socket} <- :gen_tcp.accept(listener),
           {:ok, request} <- receive_request(socket) do
        send(owner, {:raw_request, request})
        run(socket, script, owner)
      end
    end

    defp receive_request(socket, request \\ "") do
      if String.contains?(request, "\r\n\r\n") do
        {:ok, request}
      else
        case :gen_tcp.recv(socket, 0, 1_000) do
          {:ok, data} -> receive_request(socket, request <> data)
          error -> error
        end
      end
    end

    defp run(socket, script, owner) do
      Enum.each(script, fn
        {:send, data} -> :ok = :gen_tcp.send(socket, data)
        {:sleep, milliseconds} -> Process.sleep(milliseconds)
        :await_close -> send(owner, {:raw_closed, :gen_tcp.recv(socket, 0, 1_000)})
      end)

      :gen_tcp.close(socket)
    end
  end

  setup do
    {:ok, endpoint} = FakeDlrEndpoint.start_link(script: [{:reply, 200, "ACK/Jasmin"}])
    on_exit(fn -> FakeDlrEndpoint.stop(endpoint) end)
    %{endpoint: endpoint}
  end

  test "preserves exact final status across response classes", %{endpoint: endpoint} do
    Enum.each([200, 206, 302, 404, 500], fn status ->
      FakeDlrEndpoint.script(endpoint, [{:reply, status, "status-#{status}"}])

      assert {:ok, ^status, "status-" <> _value} =
               MintClient.request(context(endpoint), request(endpoint, "/status/#{status}"))
    end)

    assert length(FakeDlrEndpoint.requests(endpoint)) == 5
  end

  test "preserves GET and POST payloads", %{endpoint: endpoint} do
    FakeDlrEndpoint.script(endpoint, [
      {:reply, 200, "GET"},
      {:reply, 201, "POST"}
    ])

    assert {:ok, 200, "GET"} =
             MintClient.request(context(endpoint), request(endpoint, "/dlr?id=G1"))

    assert {:ok, 201, "POST"} =
             MintClient.request(context(endpoint), %{
               method: "POST",
               url: FakeDlrEndpoint.url(endpoint, "callback.test", "/dlr"),
               headers: [{"content-type", "application/x-www-form-urlencoded"}],
               body: "id=G1"
             })

    assert [
             %{method: "GET", path: "/dlr", body: ""},
             %{method: "POST", path: "/dlr", body: "id=G1"}
           ] = FakeDlrEndpoint.requests(endpoint)
  end

  test "does not follow redirects", %{endpoint: endpoint} do
    {:ok, target} = FakeDlrEndpoint.start_link(script: [{:reply, 200, "target"}])
    on_exit(fn -> FakeDlrEndpoint.stop(target) end)

    FakeDlrEndpoint.script(endpoint, [
      {:redirect, FakeDlrEndpoint.url(target, "callback.test", "/target")}
    ])

    assert {:ok, 302, "redirect"} =
             MintClient.request(context(endpoint), request(endpoint, "/redirect"))

    assert length(FakeDlrEndpoint.requests(endpoint)) == 1
    assert FakeDlrEndpoint.requests(target) == []
  end

  test "resolves once and sends the original host while connecting to the pinned peer", %{
    endpoint: endpoint
  } do
    resolver = {RecordingResolver, {self(), {127, 0, 0, 1}}}

    assert {:ok, 200, "ACK/Jasmin"} =
             MintClient.request(context(endpoint, resolver: resolver), request(endpoint))

    assert_receive {:resolved, "callback.test"}
    refute_receive {:resolved, "callback.test"}, 20

    assert [%{headers: headers}] = FakeDlrEndpoint.requests(endpoint)
    assert {"host", "callback.test:#{endpoint.port}"} in headers
  end

  test "returns only the final status after informational responses" do
    endpoint =
      raw_endpoint([
        {:send, "HTTP/1.1 100 Continue\r\n\r\n"},
        {:send, "HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nACK/Jasmin"},
        :await_close
      ])

    assert {:ok, 200, "ACK/Jasmin"} =
             MintClient.request(raw_context(endpoint), raw_request(endpoint))

    assert_receive {:raw_closed, {:error, :closed}}
  end

  test "rejects declared oversized bodies before receiving body data" do
    endpoint =
      raw_endpoint([
        {:send, "HTTP/1.1 200 OK\r\ncontent-length: 128\r\n\r\n"},
        :await_close
      ])

    assert {:error, :response_body_too_large} =
             MintClient.request(raw_context(endpoint, max_body_size: 64), raw_request(endpoint))

    assert_receive {:raw_closed, {:error, :closed}}
  end

  test "rejects chunked oversized bodies before accumulating the offending chunk" do
    endpoint =
      raw_endpoint([
        {:send, "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"},
        {:send, "20\r\n#{String.duplicate("a", 32)}\r\n"},
        {:send, "28\r\n#{String.duplicate("b", 40)}\r\n"},
        :await_close
      ])

    assert {:error, :response_body_too_large} =
             MintClient.request(raw_context(endpoint, max_body_size: 64), raw_request(endpoint))

    assert_receive {:raw_closed, {:error, :closed}}
  end

  test "enforces the incremental response header bound" do
    endpoint =
      raw_endpoint([
        {:send, "HTTP/1.1 200 OK\r\nx-large: #{String.duplicate("h", 128)}\r\n\r\n"},
        :await_close
      ])

    assert {:error, :response_headers_too_large} =
             MintClient.request(raw_context(endpoint, max_header_size: 64), raw_request(endpoint))

    assert_receive {:raw_closed, {:error, :closed}}
  end

  test "uses one total deadline and closes a timed-out connection" do
    endpoint =
      raw_endpoint([
        {:sleep, 100},
        :await_close
      ])

    started_at = System.monotonic_time(:millisecond)

    assert {:error, :timeout} =
             MintClient.request(raw_context(endpoint, timeout: 20), raw_request(endpoint))

    assert System.monotonic_time(:millisecond) - started_at < 100
    assert_receive {:raw_closed, {:error, :closed}}, 500
  end

  test "returns typed protocol errors and closes the connection" do
    endpoint = raw_endpoint([{:send, "not-http\r\n\r\n"}, :await_close])

    assert {:error, {:protocol, _reason}} =
             MintClient.request(raw_context(endpoint), raw_request(endpoint))

    assert_receive {:raw_closed, {:error, :closed}}
  end

  test "pins the HTTPS peer while verifying the original hostname with SNI" do
    {:ok, endpoint} =
      FakeDlrEndpoint.start_link(
        scheme: :https,
        certificate_host: "callback.test",
        script: [{:reply, 200, "ACK/Jasmin"}]
      )

    on_exit(fn -> FakeDlrEndpoint.stop(endpoint) end)

    assert {:ok, 200, "ACK/Jasmin"} =
             MintClient.request(context(endpoint), request(endpoint))

    mismatch_context =
      context(endpoint,
        resolver: resolver("wrong.test"),
        allow: [{"wrong.test", {127, 0, 0, 1}}]
      )

    mismatch_request = %{request(endpoint) | url: FakeDlrEndpoint.url(endpoint, "wrong.test")}
    assert {:error, {:tls, _reason}} = MintClient.request(mismatch_context, mismatch_request)
    assert length(FakeDlrEndpoint.requests(endpoint)) == 1
  end

  defp raw_endpoint(script) do
    endpoint = RawEndpoint.start(script, self())
    on_exit(fn -> RawEndpoint.stop(endpoint) end)
    endpoint
  end

  defp request(endpoint, path \\ "/dlr") do
    %{
      method: "GET",
      url: FakeDlrEndpoint.url(endpoint, "callback.test", path),
      headers: [],
      body: ""
    }
  end

  defp raw_request(endpoint) do
    %{method: "GET", url: RawEndpoint.url(endpoint), headers: [], body: ""}
  end

  defp raw_context(endpoint, overrides \\ []) do
    defaults = [
      resolver: resolver("callback.test"),
      allow: [{"callback.test", {127, 0, 0, 1}}],
      connect_timeout: 100,
      timeout: 500,
      max_header_size: 4_096,
      max_body_size: 65_536
    ]

    Keyword.merge(defaults, Keyword.put(overrides, :port, endpoint.port))
  end

  defp context(endpoint, overrides \\ []) do
    defaults = [
      resolver: resolver("callback.test"),
      allow: [{"callback.test", {127, 0, 0, 1}}],
      connect_timeout: 100,
      timeout: 500,
      max_header_size: 4_096,
      max_body_size: 65_536,
      cacertfile: FakeDlrEndpoint.certificate(endpoint)
    ]

    Keyword.merge(defaults, overrides)
  end

  defp resolver(host), do: {FakeDlrEndpoint.Resolver, %{host => [{127, 0, 0, 1}]}}
end
