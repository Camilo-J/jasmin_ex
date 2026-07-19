defmodule JasminEx.Smpp.PDU do
  @moduledoc """
  SMPP 3.4 PDU struct and wire encode/decode for the 16-byte header.

  Wire layout (big-endian, all 32-bit unsigned):
      +-----------------------------------------------------------+
      | command_length | command_id | command_status | sequence_n |
      |       u32      |    u32     |      u32       |    u32     |
      +-----------------------------------------------------------+
      |                  ... body bytes ...                       |
      +-----------------------------------------------------------+

  `command_length` equals 16 + byte_size(body).

  The header is the only thing this module owns. Body encode/decode and
  the per-command vocabulary live in `JasminEx.Smpp.PDU.Body` so this
  module stays a tiny pure header/struct concern.
  """

  alias JasminEx.Smpp.PDU.Constants

  @header_size 16

  @enforce_keys [:command, :status, :sequence_number, :body]
  defstruct command: nil,
            status: nil,
            sequence_number: nil,
            body: <<>>

  @type command :: atom()
  @type status :: atom()
  @type sequence_number :: non_neg_integer()
  @type body :: iodata() | binary()

  @type t :: %__MODULE__{
          command: command() | nil,
          status: status() | nil,
          sequence_number: sequence_number() | nil,
          body: body()
        }

  @doc """
  Build a PDU from a keyword list. The four required fields must all be present.
  Use `body: <<>>` for header-only PDUs (e.g. the *resp forms whose only body
  is a single optional `system_id`).
  """
  @spec build(keyword()) :: t()
  def build(opts) do
    struct!(__MODULE__, opts)
  end

  # ── encode/decode the 16-byte header ────────────────────────────────────

  @doc """
  Serialize a PDU to its SMPP wire format: 16-byte big-endian header
  followed by the body bytes. Returns iodata-friendly binary.
  """
  @spec encode(t()) :: iodata()
  def encode(%__MODULE__{} = pdu) do
    command_length = @header_size + body_byte_size(pdu.body)
    {command_id_int, status_int} = header_integers(pdu)

    [
      <<command_length::32, command_id_int::32, status_int::32, pdu.sequence_number::32>>,
      pdu.body
    ]
  end

  @doc """
  Deserialize a SMPP wire PDU into a `%PDU{}`. Returns
  `{:ok, pdu}` for a complete buffer or `{:error, {:decode, reason}}`
  for invalid input. The body byte buffer returned here is whatever
  follows the 16-byte header — typed body decoding is `Body.decode/1`.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, {:decode, atom()}}
  def decode(binary) when byte_size(binary) < @header_size, do: {:error, {:decode, :truncated}}

  def decode(
        <<command_length::32, command_id_int::32, command_status_int::32, seq::32, rest::binary>>
      )
      when command_length >= @header_size do
    body_size = command_length - @header_size

    if byte_size(rest) < body_size do
      {:error, {:decode, :truncated}}
    else
      <<body::binary-size(^body_size), _::binary>> = rest

      with {:ok, command_atom} <- Constants.command_id_to_atom(command_id_int),
           {:ok, status_atom} <- Constants.command_status_to_atom(command_status_int) do
        {:ok,
         %__MODULE__{
           command: command_atom,
           status: status_atom,
           sequence_number: seq,
           body: body
         }}
      else
        :error -> {:error, {:decode, :unknown_command_id}}
      end
    end
  end

  def decode(_other), do: {:error, {:decode, :invalid_length}}

  # ── private helpers ──────────────────────────────────────────────────────

  defp header_integers(%__MODULE__{command: command, status: status}) do
    {:ok, command_int} = Constants.command_id_to_int(command)
    {:ok, status_int} = Constants.command_status_to_int(status)
    {command_int, status_int}
  end

  # Body is iodata in principle; for length arithmetic we need a real byte size.
  # The codec only ever passes plain binaries here, so the conversion is local.
  defp body_byte_size(body) when is_binary(body), do: byte_size(body)
  defp body_byte_size(body) when is_list(body), do: IO.iodata_length(body)
  defp body_byte_size(nil), do: 0
end
