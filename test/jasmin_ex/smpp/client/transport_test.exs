defmodule JasminEx.Smpp.Client.TransportTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.Client.Transport
  alias JasminEx.Smpp.PDU

  test "connect/3 opens a binary raw active-once connection" do
    {listener, port} = listen()

    assert {:ok, socket} = Transport.connect(~c"localhost", port, 500)
    assert {:ok, peer} = accept(listener)

    assert {:ok, [mode: :binary, packet: 0, active: :once]} =
             :inet.getopts(socket, [:mode, :packet, :active])

    close_all(socket, peer, listener)
  end

  test "send/2 emits a decodable PDU preserving its sequence number" do
    {socket, peer, listener} = connected_pair()
    pdu = pdu(:enquire_link, 42)

    assert :ok = Transport.send(socket, pdu)
    assert {:ok, bytes} = :gen_tcp.recv(peer, 0, 500)
    assert {:ok, %PDU{command: :enquire_link, sequence_number: 42}} = PDU.decode(bytes)

    close_all(socket, peer, listener)
  end

  test "send/2 returns the underlying error for a closed socket" do
    {socket, peer, listener} = connected_pair()
    assert :ok = Transport.close(socket)

    assert {:error, :closed} = Transport.send(socket, pdu(:enquire_link, 1))

    close_all(peer, listener)
  end

  test "decode/2 reassembles a partial PDU" do
    bytes = encode(pdu(:enquire_link, 7))
    <<prefix::binary-size(8), suffix::binary>> = bytes

    assert {[], ^prefix} = Transport.decode(<<>>, prefix)
    assert {[%PDU{sequence_number: 7}], <<>>} = Transport.decode(prefix, suffix)
  end

  test "decode/2 preserves coalesced wire order and retains a partial tail" do
    first = encode(pdu(:enquire_link, 1))
    second = encode(pdu(:enquire_link_resp, 2))
    third = encode(pdu(:unbind, 3))
    <<tail::binary-size(9), _rest::binary>> = third

    assert {[%PDU{sequence_number: 1}, %PDU{sequence_number: 2}], ^tail} =
             Transport.decode(<<>>, first <> second <> tail)
  end

  test "decode/2 silently discards complete frames rejected by PDU.decode/1" do
    invalid = <<16::32, 0xFFFF_FFFF::32, 0::32, 9::32>>
    valid = encode(pdu(:enquire_link, 10))

    assert {[%PDU{sequence_number: 10}], <<>>} = Transport.decode(<<>>, invalid <> valid)
  end

  test "activate_once/1 re-arms delivery after the first active message" do
    {socket, peer, listener} = connected_pair()

    assert :ok = :gen_tcp.send(peer, "first")
    assert_receive {:tcp, ^socket, "first"}, 500
    assert :ok = :gen_tcp.send(peer, "second")
    refute_receive {:tcp, ^socket, "second"}, 50

    assert :ok = Transport.activate_once(socket)
    assert_receive {:tcp, ^socket, "second"}, 500

    close_all(socket, peer, listener)
  end

  test "close/1 closes a live socket" do
    {socket, peer, listener} = connected_pair()

    assert :ok = Transport.close(socket)
    assert {:error, :closed} = :gen_tcp.recv(peer, 0, 500)

    close_all(peer, listener)
  end

  defp connected_pair do
    {listener, port} = listen()
    {:ok, socket} = Transport.connect(~c"localhost", port, 500)
    {:ok, peer} = accept(listener)
    {socket, peer, listener}
  end

  defp listen do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end

  defp accept(listener) do
    parent = self()

    spawn_link(fn ->
      {:ok, peer} = :gen_tcp.accept(listener, 500)
      :ok = :gen_tcp.controlling_process(peer, parent)
      send(parent, {:accepted, peer})
    end)

    receive do
      {:accepted, peer} -> {:ok, peer}
    after
      500 -> {:error, :timeout}
    end
  end

  defp pdu(command, sequence_number) do
    PDU.build(
      command: command,
      status: :ESME_ROK,
      sequence_number: sequence_number,
      body: <<>>
    )
  end

  defp encode(pdu), do: pdu |> PDU.encode() |> IO.iodata_to_binary()

  defp close_all(sockets) when is_list(sockets), do: Enum.each(sockets, &:gen_tcp.close/1)
  defp close_all(first, second), do: close_all([first, second])
  defp close_all(first, second, third), do: close_all([first, second, third])
end
