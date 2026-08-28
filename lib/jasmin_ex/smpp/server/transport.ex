defmodule JasminEx.Smpp.Server.Transport do
  @moduledoc false

  alias JasminEx.Smpp.Framing
  alias JasminEx.Smpp.PDU

  def listen(port, host \\ {127, 0, 0, 1}) do
    :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true, ip: host])
  end

  def accept(listener), do: :gen_tcp.accept(listener)
  def own(socket), do: :inet.setopts(socket, active: :once)
  def activate(socket), do: :inet.setopts(socket, active: :once)
  def close(socket), do: :gen_tcp.close(socket)
  def send_pdu(socket, pdu), do: :gen_tcp.send(socket, IO.iodata_to_binary(PDU.encode(pdu)))
  def feed(buffer, chunk, max), do: Framing.feed_strict(buffer, chunk, max)
end
