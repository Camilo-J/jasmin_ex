defmodule JasminEx.Smpp.Client.Transport do
  @moduledoc false

  alias JasminEx.Smpp.Framing
  alias JasminEx.Smpp.PDU

  @spec connect(:inet.hostname() | :inet.ip_address(), :inet.port_number(), timeout()) ::
          {:ok, :gen_tcp.socket()} | {:error, term()}
  def connect(host, port, timeout) do
    :gen_tcp.connect(host, port, [:binary, packet: :raw, active: :once], timeout)
  end

  @spec send(:gen_tcp.socket(), PDU.t()) :: :ok | {:error, term()}
  def send(socket, %PDU{} = pdu) do
    pdu
    |> PDU.encode()
    |> IO.iodata_to_binary()
    |> then(&:gen_tcp.send(socket, &1))
  end

  @spec decode(binary(), binary()) :: {[PDU.t()], binary()}
  def decode(buffer, chunk) do
    {frames, leftover} = Framing.feed(buffer, chunk)

    pdus =
      Enum.flat_map(frames, fn frame ->
        case PDU.decode(frame) do
          {:ok, pdu} -> [pdu]
          {:error, _reason} -> []
        end
      end)

    {pdus, leftover}
  end

  @spec activate_once(:gen_tcp.socket()) :: :ok | {:error, term()}
  def activate_once(socket), do: :inet.setopts(socket, active: :once)

  @spec close(:gen_tcp.socket()) :: :ok
  def close(socket), do: :gen_tcp.close(socket)
end
