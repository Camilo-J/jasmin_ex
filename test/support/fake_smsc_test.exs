defmodule JasminEx.Smpp.FakeSMSCTest do
  @moduledoc false
  # Sanity tests for the FakeSMSC TCP peer harness. Real behavior coverage
  # lives in `JasminEx.Smpp.ClientTest`. These tests only prove the harness
  # itself can:
  #   * bind a local port and accept one connection
  #   * deliver bytes written by the harness to a connected client socket
  #   * deliver bytes written by the connected client to the harness handler
  #   * shut down cleanly (no leaked listener / handler processes)
  use ExUnit.Case, async: false

  alias JasminEx.Smpp.FakeSMSC

  defp free_port do
    {:ok, l} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(l)
    :ok = :gen_tcp.close(l)
    port
  end

  defp connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false], 1_000)
    sock
  end

  describe "TCP peer plumbing" do
    test "start_link/1 returns the bound port for the harness" do
      port = free_port()
      assert {:ok, ^port, pid} = FakeSMSC.start_link(port: port)
      assert is_pid(pid)
      refute Process.alive?(pid) == false
      :ok = GenServer.stop(pid)
    end

    test "a TCP client can connect to the harness port" do
      port = free_port()
      {:ok, ^port, pid} = FakeSMSC.start_link(port: port)
      sock = connect(port)
      assert :ok = :gen_tcp.close(sock)
      :ok = GenServer.stop(pid)
    end

    test "bytes written by the harness reach the connected client" do
      port = free_port()
      {:ok, ^port, pid} = FakeSMSC.start_link(port: port)
      sock = connect(port)
      :ok = FakeSMSC.wait_connected(pid)

      assert :ok = FakeSMSC.send_bytes(pid, "hello")
      assert {:ok, "hello"} = :gen_tcp.recv(sock, 0, 1_000)

      :ok = :gen_tcp.close(sock)
      :ok = GenServer.stop(pid)
    end

    test "bytes written by the connected client reach the harness handler" do
      port = free_port()
      {:ok, ^port, pid} = FakeSMSC.start_link(port: port)
      sock = connect(port)
      :ok = FakeSMSC.wait_connected(pid)
      ref = FakeSMSC.subscribe(pid)

      assert :ok = :gen_tcp.send(sock, "from-client")
      assert_receive {:fake_smsc_bytes, ^ref, "from-client"}, 1_000

      :ok = :gen_tcp.close(sock)
      :ok = GenServer.stop(pid)
    end

    test "stopping the harness closes any live client sockets" do
      port = free_port()
      {:ok, ^port, pid} = FakeSMSC.start_link(port: port)
      sock = connect(port)
      :ok = GenServer.stop(pid)
      Process.sleep(20)
      assert {:error, :closed} = :gen_tcp.recv(sock, 0, 200)
    end
  end
end
