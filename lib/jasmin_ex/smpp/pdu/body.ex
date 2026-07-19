defmodule JasminEx.Smpp.PDU.Body do
  @moduledoc """
  SMPP 3.4 per-command body encode/decode for the in-scope command set.

  Body shape summary
  ───────────────────
    * `bind_*` (transmitter / receiver / transceiver)
        system_id c-octet, password c-octet, system_type c-octet,
        interface_version u8, addr_ton u8, addr_npi u8, address_range c-octet
    * `bind_*_resp`
        system_id c-octet (single field; may be empty)
    * `unbind` / `unbind_resp`
        empty
    * `enquire_link` / `enquire_link_resp`
        empty
    * `submit_sm` / `deliver_sm`
        16 mandatory fixed/var fields then `sm_length` u8 plus `short_message`
        bytes (later via `PDU.Coding`)
    * `submit_sm_resp` / `deliver_sm_resp`
        message_id c-octet
    * `generic_nack`
        empty

  C-Octet = null-terminated string. Field terminators are validated on
  decode so a malformed PDU returns `{:error, {:decode, :bad_cstring}}`
  rather than crashing or producing a silent partial parse.

  `Body.encode/2` and `Body.decode/2` operate ONLY on the body bytes —
  the 16-byte header is encoded/decoded by `JasminEx.Smpp.PDU`. The
  full pipeline combines the two.
  """

  alias JasminEx.Smpp.PDU.Constants

  # ── nested structs ───────────────────────────────────────────────────────

  defmodule Bind do
    @moduledoc "bind_{transmitter,receiver,transceiver} body."
    defstruct system_id: nil,
              password: nil,
              system_type: nil,
              interface_version: nil,
              addr_ton: nil,
              addr_npi: nil,
              address_range: nil

    @type t :: %__MODULE__{
            system_id: String.t(),
            password: String.t(),
            system_type: String.t(),
            interface_version: non_neg_integer(),
            addr_ton: atom(),
            addr_npi: atom(),
            address_range: String.t()
          }
  end

  defmodule BindResp do
    @moduledoc "bind_*_resp body — just a system_id c-octet."
    defstruct system_id: ""
    @type t :: %__MODULE__{system_id: String.t()}
  end

  defmodule SubmitSM do
    @moduledoc "submit_sm body."
    defstruct service_type: "",
              source_addr_ton: :UNKNOWN,
              source_addr_npi: :UNKNOWN,
              source_addr: "",
              dest_addr_ton: :UNKNOWN,
              dest_addr_npi: :UNKNOWN,
              destination_addr: "",
              esm_class: 0,
              protocol_id: 0,
              priority_flag: 0,
              schedule_delivery_time: "",
              validity_period: "",
              registered_delivery: 0,
              replace_if_present_flag: 0,
              data_coding: :SMSC_DEFAULT_ALPHABET,
              sm_default_msg_id: 0,
              short_message: ""

    @type t :: %__MODULE__{
            service_type: String.t(),
            source_addr_ton: atom(),
            source_addr_npi: atom(),
            source_addr: String.t(),
            dest_addr_ton: atom(),
            dest_addr_npi: atom(),
            destination_addr: String.t(),
            esm_class: non_neg_integer(),
            protocol_id: non_neg_integer(),
            priority_flag: non_neg_integer(),
            schedule_delivery_time: String.t(),
            validity_period: String.t(),
            registered_delivery: non_neg_integer(),
            replace_if_present_flag: non_neg_integer(),
            data_coding: atom() | non_neg_integer(),
            sm_default_msg_id: non_neg_integer(),
            short_message: String.t()
          }
  end

  defmodule SubmitSMResp do
    @moduledoc "submit_sm_resp / deliver_sm_resp body — message_id c-octet."
    defstruct message_id: ""
    @type t :: %__MODULE__{message_id: String.t()}
  end

  defmodule DeliverSM do
    @moduledoc "deliver_sm body — same shape as submit_sm."
    defstruct service_type: "",
              source_addr_ton: :UNKNOWN,
              source_addr_npi: :UNKNOWN,
              source_addr: "",
              dest_addr_ton: :UNKNOWN,
              dest_addr_npi: :UNKNOWN,
              destination_addr: "",
              esm_class: 0,
              protocol_id: 0,
              priority_flag: 0,
              schedule_delivery_time: "",
              validity_period: "",
              registered_delivery: 0,
              replace_if_present_flag: 0,
              data_coding: :SMSC_DEFAULT_ALPHABET,
              sm_default_msg_id: 0,
              short_message: ""

    @type t :: %__MODULE__{
            service_type: String.t(),
            source_addr_ton: atom(),
            source_addr_npi: atom(),
            source_addr: String.t(),
            dest_addr_ton: atom(),
            dest_addr_npi: atom(),
            destination_addr: String.t(),
            esm_class: non_neg_integer(),
            protocol_id: non_neg_integer(),
            priority_flag: non_neg_integer(),
            schedule_delivery_time: String.t(),
            validity_period: String.t(),
            registered_delivery: non_neg_integer(),
            replace_if_present_flag: non_neg_integer(),
            data_coding: atom() | non_neg_integer(),
            sm_default_msg_id: non_neg_integer(),
            short_message: String.t()
          }
  end

  defmodule DeliverSMResp do
    @moduledoc "deliver_sm_resp body — message_id c-octet."
    defstruct message_id: ""
    @type t :: %__MODULE__{message_id: String.t()}
  end

  defmodule Unbind do
    @moduledoc "unbind body — empty."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule EnquireLink do
    @moduledoc "enquire_link body — empty."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule GenericNack do
    @moduledoc "generic_nack body — empty."
    defstruct []
    @type t :: %__MODULE__{}
  end

  # ── encode ────────────────────────────────────────────────────────────────

  @typedoc "Any body struct from above."
  @type t ::
          Bind.t()
          | BindResp.t()
          | SubmitSM.t()
          | SubmitSMResp.t()
          | DeliverSM.t()
          | DeliverSMResp.t()
          | Unbind.t()
          | EnquireLink.t()
          | GenericNack.t()

  @spec encode(atom(), t()) :: {:ok, iodata()} | {:error, {:encode, atom()}}
  def encode(:bind_transmitter, %Bind{} = b), do: {:ok, encode_bind(b)}
  def encode(:bind_receiver, %Bind{} = b), do: {:ok, encode_bind(b)}
  def encode(:bind_transceiver, %Bind{} = b), do: {:ok, encode_bind(b)}
  def encode(:bind_transmitter_resp, %BindResp{} = b), do: {:ok, encode_c_octet(b.system_id)}
  def encode(:bind_receiver_resp, %BindResp{} = b), do: {:ok, encode_c_octet(b.system_id)}
  def encode(:bind_transceiver_resp, %BindResp{} = b), do: {:ok, encode_c_octet(b.system_id)}
  def encode(:unbind, %Unbind{}), do: {:ok, <<>>}
  def encode(:unbind_resp, %Unbind{}), do: {:ok, <<>>}
  def encode(:enquire_link, %EnquireLink{}), do: {:ok, <<>>}
  def encode(:enquire_link_resp, %EnquireLink{}), do: {:ok, <<>>}
  def encode(:submit_sm, %SubmitSM{} = b), do: encode_submit_sm(b)
  def encode(:submit_sm_resp, %SubmitSMResp{} = b), do: {:ok, encode_c_octet(b.message_id)}
  def encode(:deliver_sm, %DeliverSM{} = b), do: encode_submit_sm(b)
  def encode(:deliver_sm_resp, %DeliverSMResp{} = b), do: {:ok, encode_c_octet(b.message_id)}
  def encode(:generic_nack, %GenericNack{}), do: {:ok, <<>>}
  def encode(_other, _body), do: {:error, {:encode, :unknown_command}}

  # ── decode ────────────────────────────────────────────────────────────────

  @type decode_result :: {:ok, t()} | {:error, {:decode, atom()}}

  @spec decode(atom(), binary()) :: decode_result()
  def decode(:bind_transmitter, bin), do: decode_bind(bin)
  def decode(:bind_receiver, bin), do: decode_bind(bin)
  def decode(:bind_transceiver, bin), do: decode_bind(bin)
  def decode(:bind_transmitter_resp, bin), do: decode_resp_bind(bin)
  def decode(:bind_receiver_resp, bin), do: decode_resp_bind(bin)
  def decode(:bind_transceiver_resp, bin), do: decode_resp_bind(bin)
  def decode(:unbind, <<>>), do: {:ok, %Unbind{}}
  def decode(:unbind_resp, <<>>), do: {:ok, %Unbind{}}
  def decode(:enquire_link, <<>>), do: {:ok, %EnquireLink{}}
  def decode(:enquire_link_resp, <<>>), do: {:ok, %EnquireLink{}}
  def decode(:submit_sm, bin), do: decode_submit_sm(bin, :submit_sm)
  def decode(:submit_sm_resp, bin), do: decode_c_octet_body(bin, &%SubmitSMResp{message_id: &1})
  def decode(:deliver_sm, bin), do: decode_submit_sm(bin, :deliver_sm)
  def decode(:deliver_sm_resp, bin), do: decode_c_octet_body(bin, &%DeliverSMResp{message_id: &1})
  def decode(:generic_nack, <<>>), do: {:ok, %GenericNack{}}
  def decode(:unbind, _), do: {:error, {:decode, :truncated}}
  def decode(:unbind_resp, _), do: {:error, {:decode, :truncated}}
  def decode(:enquire_link, _), do: {:error, {:decode, :truncated}}
  def decode(:enquire_link_resp, _), do: {:error, {:decode, :truncated}}
  def decode(:generic_nack, _), do: {:error, {:decode, :truncated}}
  def decode(_other, _bin), do: {:error, {:decode, :unknown_command}}

  # ── private: bind shape ───────────────────────────────────────────────────

  defp encode_bind(%Bind{} = b) do
    [
      encode_c_octet(b.system_id),
      encode_c_octet(b.password),
      encode_c_octet(b.system_type),
      <<b.interface_version::8>>,
      encode_ton(b.addr_ton),
      encode_npi(b.addr_npi),
      encode_c_octet(b.address_range)
    ]
    |> IO.iodata_to_binary()
  end

  defp decode_bind(bin) do
    with {:ok, system_id, rest1} <- split_c_octet(bin, "system_id"),
         {:ok, password, rest2} <- split_c_octet(rest1, "password"),
         {:ok, system_type, rest3} <- split_c_octet(rest2, "system_type"),
         {:ok, interface_version, rest4} <- take_u8(rest3, "interface_version"),
         {:ok, addr_ton_int, rest5} <- take_u8(rest4, "addr_ton"),
         {:ok, addr_npi_int, rest6} <- take_u8(rest5, "addr_npi"),
         {:ok, address_range, _rest7} <- split_c_octet(rest6, "address_range") do
      {:ok,
       %Bind{
         system_id: system_id,
         password: password,
         system_type: system_type,
         interface_version: interface_version,
         addr_ton: atom_or_int(addr_ton_int, Constants.ton_to_atom(addr_ton_int)),
         addr_npi: atom_or_int(addr_npi_int, Constants.npi_to_atom(addr_npi_int)),
         address_range: address_range
       }}
    end
  end

  defp decode_resp_bind(bin) do
    case split_c_octet(bin, "system_id") do
      {:ok, system_id, _rest} -> {:ok, %BindResp{system_id: system_id}}
      err -> err
    end
  end

  # ── private: submit_sm / deliver_sm shape ──────────────────────────────────
  #
  # 16 mandatory fields: 6 c-octets + 3 u8 + 2 c-octets + 2 u8 + 2 u8 + 1 u8
  # then: sm_length u8 + sm bytes

  defp encode_submit_sm(b) do
    sm_bytes = b.short_message || ""

    sm_int =
      if is_atom(b.data_coding), do: atom_to_data_coding_int(b.data_coding), else: b.data_coding

    bytes =
      [
        encode_c_octet(b.service_type),
        encode_ton(b.source_addr_ton),
        encode_npi(b.source_addr_npi),
        encode_c_octet(b.source_addr),
        encode_ton(b.dest_addr_ton),
        encode_npi(b.dest_addr_npi),
        encode_c_octet(b.destination_addr),
        <<b.esm_class::8>>,
        <<b.protocol_id::8>>,
        <<b.priority_flag::8>>,
        encode_c_octet(b.schedule_delivery_time),
        encode_c_octet(b.validity_period),
        <<b.registered_delivery::8>>,
        <<b.replace_if_present_flag::8>>,
        <<sm_int::8>>,
        <<b.sm_default_msg_id::8>>,
        <<byte_size(sm_bytes)::8>>,
        sm_bytes
      ]

    {:ok, IO.iodata_to_binary(bytes)}
  end

  defp decode_submit_sm(bin, shape) do
    with {:ok, service_type, rest} <- split_c_octet(bin, "service_type"),
         {:ok, sat, rest} <- take_u8(rest, "source_addr_ton"),
         {:ok, san, rest} <- take_u8(rest, "source_addr_npi"),
         {:ok, source_addr, rest} <- split_c_octet(rest, "source_addr"),
         {:ok, dat, rest} <- take_u8(rest, "dest_addr_ton"),
         {:ok, dan, rest} <- take_u8(rest, "dest_addr_npi"),
         {:ok, destination_addr, rest} <- split_c_octet(rest, "destination_addr"),
         {:ok, esm_class, rest} <- take_u8(rest, "esm_class"),
         {:ok, protocol_id, rest} <- take_u8(rest, "protocol_id"),
         {:ok, priority_flag, rest} <- take_u8(rest, "priority_flag"),
         {:ok, schedule_delivery_time, rest} <- split_c_octet(rest, "schedule_delivery_time"),
         {:ok, validity_period, rest} <- split_c_octet(rest, "validity_period"),
         {:ok, registered_delivery, rest} <- take_u8(rest, "registered_delivery"),
         {:ok, replace_if_present_flag, rest} <- take_u8(rest, "replace_if_present_flag"),
         {:ok, data_coding, rest} <- take_u8(rest, "data_coding"),
         {:ok, sm_default_msg_id, rest} <- take_u8(rest, "sm_default_msg_id"),
         {:ok, sm_length, rest} <- take_u8(rest, "sm_length") do
      build_submit_sm(rest, sm_length, shape, %{
        service_type: service_type,
        source_addr_ton: ton_atom(sat),
        source_addr_npi: npi_atom(san),
        source_addr: source_addr,
        dest_addr_ton: ton_atom(dat),
        dest_addr_npi: npi_atom(dan),
        destination_addr: destination_addr,
        esm_class: esm_class,
        protocol_id: protocol_id,
        priority_flag: priority_flag,
        schedule_delivery_time: schedule_delivery_time,
        validity_period: validity_period,
        registered_delivery: registered_delivery,
        replace_if_present_flag: replace_if_present_flag,
        data_coding: data_coding_atom(data_coding),
        sm_default_msg_id: sm_default_msg_id
      })
    end
  end

  # Guard clause: not enough bytes for short_message — early return with error
  defp build_submit_sm(rest, sm_length, _shape, _fields)
       when byte_size(rest) < sm_length do
    {:error, {:decode, :truncated}}
  end

  # Happy path: extract short_message and build the right struct based on shape
  defp build_submit_sm(rest, sm_length, shape, fields) do
    <<short_message::binary-size(^sm_length), _::binary>> = rest
    {:ok, build_submit_struct(shape, Map.put(fields, :short_message, short_message))}
  end

  # Struct dispatch — SubmitSM and DeliverSM share every SMPP 3.4 body field,
  # so only the wrapping struct varies. Using `struct/2` with a map tail
  # gives full field coverage without a giant %Struct{...} literal.
  defp build_submit_struct(:submit_sm, fields), do: struct(%SubmitSM{}, fields)
  defp build_submit_struct(:deliver_sm, fields), do: struct(%DeliverSM{}, fields)

  # ── private: low-level binary helpers ────────────────────────────────────

  defp encode_c_octet(str) when is_binary(str) do
    [str, 0]
  end

  defp encode_c_octet(nil), do: [0]

  defp split_c_octet(bin, _name) do
    case :binary.split(bin, <<0>>) do
      [str, rest] -> {:ok, str, rest}
      [_str] -> {:error, {:decode, :bad_cstring}}
    end
  end

  defp take_u8(bin, _field) when byte_size(bin) >= 1 do
    <<byte::8, rest::binary>> = bin
    {:ok, byte, rest}
  end

  defp take_u8(_bin, _field), do: {:error, {:decode, :truncated}}

  defp decode_c_octet_body(bin, struct_fn) do
    case split_c_octet(bin, "message_id") do
      {:ok, message_id, _rest} -> {:ok, struct_fn.(message_id)}
      err -> err
    end
  end

  defp encode_ton(:UNKNOWN), do: <<0>>
  defp encode_ton(int) when is_integer(int), do: <<int::8>>

  defp encode_ton(atom) when is_atom(atom) do
    case Constants.ton_to_int(atom) do
      {:ok, int} -> <<int::8>>
      :error -> <<0>>
    end
  end

  defp encode_npi(:UNKNOWN), do: <<0>>
  defp encode_npi(int) when is_integer(int), do: <<int::8>>

  defp encode_npi(atom) when is_atom(atom) do
    case Constants.npi_to_int(atom) do
      {:ok, int} -> <<int::8>>
      :error -> <<0>>
    end
  end

  defp ton_atom(int), do: atom_or_int(int, Constants.ton_to_atom(int))
  defp npi_atom(int), do: atom_or_int(int, Constants.npi_to_atom(int))

  defp atom_or_int(_int, {:ok, atom}), do: atom
  defp atom_or_int(int, :error), do: int

  defp data_coding_atom(int) when is_integer(int) do
    case Constants.data_coding_to_atom(int) do
      {:ok, atom} -> atom
      :error -> int
    end
  end

  defp atom_to_data_coding_int(atom) when is_atom(atom) do
    case Constants.data_coding_to_int(atom) do
      {:ok, n} -> n
      :error -> 0
    end
  end
end
