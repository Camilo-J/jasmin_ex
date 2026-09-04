defmodule JasminEx.HttpApi.ListenerTest do
  use ExUnit.Case, async: false

  alias JasminEx.HttpApi.Config
  alias JasminEx.HttpApi.Supervisor, as: HttpSupervisor

  test "occupied port fails the supervised child" do
    {:ok, occupied} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: false, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(occupied)
    unique = System.unique_integer([:positive])

    on_exit(fn ->
      :gen_tcp.close(occupied)
    end)

    result =
      start_supervised(
        {HttpSupervisor,
         [
           config: Config.new(enabled: true, host: {127, 0, 0, 1}, port: port),
           metrics: :"http-metrics-busy-#{unique}"
         ]}
      )

    assert {:error, reason} = result
    assert eaddrinuse?(reason)
  end

  test "enabled port 0 serves documented routes" do
    unique = System.unique_integer([:positive])

    sup =
      start_supervised!(
        {HttpSupervisor,
         [config: Config.new(enabled: true, port: 0), metrics: :"http-metrics-#{unique}"]}
      )

    port = HttpSupervisor.port(sup)
    ping = http_get(port, "/ping")
    send_get = http_get(port, "/send")

    assert ping.status == 200
    assert ping.body == "pong\n"
    assert ping.content_type =~ "text/plain"

    assert send_get.status == 405
    assert send_get.body == "error:method_not_allowed\n"
  end

  defp eaddrinuse?(:eaddrinuse), do: true

  defp eaddrinuse?(tuple) when is_tuple(tuple),
    do: Enum.any?(Tuple.to_list(tuple), &eaddrinuse?/1)

  defp eaddrinuse?(_reason), do: false

  defp http_get(port, path) do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, packet: :raw, active: false], 1_000)

    :ok =
      :gen_tcp.send(
        socket,
        "GET #{path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
      )

    raw = recv_all(socket, [])
    :gen_tcp.close(socket)
    parse_http(raw)
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_000) do
      {:ok, data} -> recv_all(socket, [acc, data])
      {:error, :closed} -> IO.iodata_to_binary(acc)
    end
  end

  defp parse_http(raw) do
    [header, body] = String.split(raw, "\r\n\r\n", parts: 2)
    [status_line | headers] = String.split(header, "\r\n")
    ["HTTP/" <> _version, status | _reason] = String.split(status_line, " ", parts: 3)

    %{status: String.to_integer(status), body: body, content_type: content_type(headers)}
  end

  defp content_type(headers) do
    Enum.find_value(headers, "", &content_type_header/1)
  end

  defp content_type_header(line) do
    case String.split(line, ":", parts: 2) do
      [name, value] -> typed_header(name, value)
      _other -> nil
    end
  end

  defp typed_header(name, value) do
    case String.downcase(name) do
      "content-type" -> String.trim(value)
      _other -> nil
    end
  end
end
