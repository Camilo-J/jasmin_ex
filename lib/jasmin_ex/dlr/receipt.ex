defmodule JasminEx.Dlr.Receipt do
  @moduledoc false

  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.PDU.Tlv

  defstruct id: nil,
            stat: nil,
            sub: "ND",
            dlvrd: "ND",
            sdate: "ND",
            ddate: "ND",
            err: "ND",
            text: ""

  @type t :: %__MODULE__{
          id: binary() | nil,
          stat: binary() | nil,
          sub: binary(),
          dlvrd: binary(),
          sdate: binary(),
          ddate: binary(),
          err: binary(),
          text: binary()
        }

  @state_map %{
    2 => "DELIVRD",
    3 => "EXPIRED",
    4 => "DELETED",
    5 => "UNDELIV",
    6 => "ACCEPTD",
    7 => "UNKNOWN",
    8 => "REJECTD"
  }

  @patterns [
    ~r/id:(?<id>[\dA-Za-z-_]+)/,
    ~r/sub:(?<sub>\d{1,3})/,
    ~r/dlvrd:(?<dlvrd>\d{1,3})/,
    ~r/submit date:(?<sdate>\d+)/,
    ~r/done date:(?<ddate>\d+)/,
    ~r/stat:(?<stat>\w{7})/,
    ~r/err:(?<err>\w{1,3})/,
    ~r/[tT]ext:(?<text>.*)/
  ]

  @spec parse(term()) :: {:ok, t()} | :not_dlr | {:error, atom()}
  def parse(%Body.DeliverSM{} = pdu) do
    with {:ok, tlvs} <- Tlv.decode(pdu.optional_parameters) do
      classify(tlv_fields(tlvs), text_fields(pdu.short_message))
    end
  end

  def parse(_other), do: :not_dlr

  defp tlv_fields(tlvs) do
    Enum.reduce(tlvs, %{}, fn
      {:receipted_message_id, id}, acc -> Map.put(acc, :id, id)
      {:message_state, state}, acc -> Map.put(acc, :stat, Map.get(@state_map, state, "UNKNOWN"))
      _other, acc -> acc
    end)
  end

  defp text_fields(short_message) do
    text = utf8_ignore(short_message)

    Enum.reduce(@patterns, %{}, fn regex, acc ->
      case Regex.named_captures(regex, text) do
        nil -> acc
        captures -> merge_text(acc, captures)
      end
    end)
  end

  defp merge_text(acc, captures) do
    Enum.reduce(captures, acc, fn {key, value}, acc ->
      case field_key(key) do
        {:ok, atom} -> Map.put_new(acc, atom, value)
        :error -> acc
      end
    end)
  end

  defp field_key("id"), do: {:ok, :id}
  defp field_key("sub"), do: {:ok, :sub}
  defp field_key("dlvrd"), do: {:ok, :dlvrd}
  defp field_key("sdate"), do: {:ok, :sdate}
  defp field_key("ddate"), do: {:ok, :ddate}
  defp field_key("stat"), do: {:ok, :stat}
  defp field_key("err"), do: {:ok, :err}
  defp field_key("text"), do: {:ok, :text}
  defp field_key(_other), do: :error

  defp classify(tlv, text) do
    fields =
      text
      |> Map.merge(Map.take(tlv, [:id, :stat]))
      |> pad_numeric()

    case fields do
      %{id: id, stat: stat} when is_binary(id) and is_binary(stat) and id != "" and stat != "" ->
        {:ok,
         %__MODULE__{
           id: id,
           stat: stat,
           sub: Map.get(fields, :sub, "ND"),
           dlvrd: Map.get(fields, :dlvrd, "ND"),
           sdate: Map.get(fields, :sdate, "ND"),
           ddate: Map.get(fields, :ddate, "ND"),
           err: Map.get(fields, :err, "ND"),
           text: Map.get(fields, :text, "")
         }}

      _missing ->
        :not_dlr
    end
  end

  defp pad_numeric(fields) do
    fields
    |> pad(:sub)
    |> pad(:dlvrd)
    |> pad(:err)
  end

  defp pad(fields, key) do
    case Map.get(fields, key) do
      value when is_binary(value) and value != "ND" and byte_size(value) < 3 ->
        Map.put(fields, key, String.pad_leading(value, 3, "0"))

      _other ->
        fields
    end
  end

  defp utf8_ignore(<<>>), do: <<>>

  defp utf8_ignore(bin) when is_binary(bin) do
    case :unicode.characters_to_binary(bin) do
      out when is_binary(out) -> out
      {:error, good, <<_skip, rest::binary>>} -> good <> utf8_ignore(rest)
      {:error, good, <<>>} -> good
      {:incomplete, good, _rest} -> good
    end
  end
end
