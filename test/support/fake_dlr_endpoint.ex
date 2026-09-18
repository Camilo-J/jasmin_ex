defmodule JasminEx.FakeDlrEndpoint do
  @moduledoc false

  defmodule Resolver do
    @moduledoc false
    def resolve(table, host), do: {:ok, Map.fetch!(table, host)}
  end

  defmodule Plug do
    @moduledoc false
    import Elixir.Plug.Conn

    def init(agent), do: agent

    def call(conn, agent) do
      {:ok, body, conn} = read_body(conn)

      request = %{
        method: conn.method,
        path: conn.request_path,
        body: body,
        headers: conn.req_headers
      }

      action =
        Agent.get_and_update(agent, fn state ->
          [action | rest] = state.script
          {action, %{state | script: rest, requests: [request | state.requests]}}
        end)

      respond(conn, action)
    end

    defp respond(conn, {:reply, status, body}), do: send_resp(conn, status, body)

    defp respond(conn, {:redirect, location}) do
      conn
      |> put_resp_header("location", location)
      |> send_resp(302, "redirect")
    end

    defp respond(conn, {:slow, milliseconds, status, body}) do
      Process.sleep(milliseconds)
      send_resp(conn, status, body)
    end

    defp respond(_conn, {:disconnect, :before_response}), do: exit(:scripted_disconnect)
  end

  def start_link(opts) do
    scheme = Keyword.get(opts, :scheme, :http)

    {:ok, agent} =
      Agent.start_link(fn -> %{script: Keyword.fetch!(opts, :script), requests: []} end)

    {tls_options, certificate, directory} = tls_options(scheme, opts)

    bandit_opts = [
      plug: {Plug, agent},
      scheme: scheme,
      ip: {127, 0, 0, 1},
      port: 0,
      thousand_island_options: tls_options
    ]

    case Bandit.start_link(bandit_opts) do
      {:ok, server} ->
        {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

        {:ok,
         %{
           server: server,
           agent: agent,
           scheme: scheme,
           port: port,
           certificate: certificate,
           directory: directory
         }}

      error ->
        Agent.stop(agent)
        error
    end
  end

  def stop(endpoint) do
    safe_stop(endpoint.server, &GenServer.stop/1)
    safe_stop(endpoint.agent, &Agent.stop/1)
    if endpoint.directory, do: File.rm_rf!(endpoint.directory)
    :ok
  end

  def script(endpoint, actions) when is_list(actions) do
    Agent.update(endpoint.agent, &%{&1 | script: actions})
  end

  def requests(endpoint), do: Agent.get(endpoint.agent, &Enum.reverse(&1.requests))
  def certificate(endpoint), do: endpoint.certificate

  def url(endpoint, host, path \\ "/dlr") do
    "#{endpoint.scheme}://#{host}:#{endpoint.port}#{path}"
  end

  defp tls_options(:http, _opts), do: {[], nil, nil}

  defp tls_options(:https, opts) do
    host = Keyword.fetch!(opts, :certificate_host)

    directory =
      Path.join(System.tmp_dir!(), "jasmin-ex-dlr-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    ca_certificate = Path.join(directory, "ca.pem")
    ca_key = Path.join(directory, "ca-key.pem")
    certificate = Path.join(directory, "certificate.pem")
    key = Path.join(directory, "key.pem")
    request = Path.join(directory, "request.pem")
    extensions = Path.join(directory, "extensions.cnf")

    run_openssl!([
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      ca_key,
      "-out",
      ca_certificate,
      "-days",
      "1",
      "-subj",
      "/CN=JasminEx Test CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
      "-addext",
      "keyUsage=critical,keyCertSign,cRLSign"
    ])

    run_openssl!([
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      key,
      "-out",
      request,
      "-subj",
      "/CN=#{host}"
    ])

    File.write!(extensions, "subjectAltName=DNS:#{host}\nbasicConstraints=critical,CA:FALSE\n")

    run_openssl!([
      "x509",
      "-req",
      "-in",
      request,
      "-CA",
      ca_certificate,
      "-CAkey",
      ca_key,
      "-CAcreateserial",
      "-out",
      certificate,
      "-days",
      "1",
      "-extfile",
      extensions
    ])

    options = [transport_options: [certfile: certificate, keyfile: key]]
    {options, ca_certificate, directory}
  end

  defp run_openssl!(arguments) do
    {output, status} = System.cmd("openssl", arguments, stderr_to_stdout: true)
    if status != 0, do: raise("failed to create test certificate: #{output}")
  end

  defp safe_stop(pid, stop) do
    if Process.alive?(pid), do: stop.(pid)
  catch
    :exit, _reason -> :ok
  end
end
