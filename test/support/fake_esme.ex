defmodule JasminEx.Smpp.FakeESME do
  @moduledoc false
  alias JasminEx.Smpp.{Framing, PDU, PDU.Body}

  def connect(port),
    do: :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, packet: :raw, active: false], 1_000)

  def send_raw(sock, bin), do: :gen_tcp.send(sock, bin)
  def close(sock), do: :gen_tcp.close(sock)

  def send_pdu(sock, pdu), do: :gen_tcp.send(sock, IO.iodata_to_binary(PDU.encode(pdu)))

  def recv_pdu(sock, timeout \\ 1_000, buf \\ <<>>) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, bin} ->
        case Framing.feed(buf, bin) do
          {[frame | _], _} -> PDU.decode(frame)
          {[], leftover} -> recv_pdu(sock, timeout, leftover)
        end

      err ->
        err
    end
  end

  def request(sock, command, seq, body \\ <<>>) do
    :ok =
      send_pdu(
        sock,
        PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: body)
      )

    recv_pdu(sock)
  end

  def bind(sock, command, system_id, password, seq \\ 1) do
    {:ok, body} =
      Body.encode(command, %Body.Bind{
        system_id: system_id,
        password: password,
        system_type: "",
        interface_version: 0x34,
        addr_ton: :UNKNOWN,
        addr_npi: :UNKNOWN,
        address_range: ""
      })

    request(sock, command, seq, body)
  end

  def enquire_link(sock, seq \\ 2), do: request(sock, :enquire_link, seq)
  def unbind(sock, seq \\ 3), do: request(sock, :unbind, seq)

  def submit_sm(sock, seq \\ 4) do
    {:ok, body} = Body.encode(:submit_sm, %Body.SubmitSM{short_message: "hi"})
    request(sock, :submit_sm, seq, body)
  end
end
