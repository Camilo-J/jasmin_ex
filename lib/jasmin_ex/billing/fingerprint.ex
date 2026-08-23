defmodule JasminEx.Billing.Fingerprint do
  @moduledoc false

  alias JasminEx.Billing.Bill

  @enforce_keys [:version, :digest]
  defstruct @enforce_keys

  @type t :: %__MODULE__{version: 1, digest: binary()}

  @spec compute(Bill.t()) :: {:ok, t()}
  def compute(%Bill{} = bill) do
    {:ok, %__MODULE__{version: 1, digest: :crypto.hash(:sha256, canonical_payload(bill))}}
  end

  defp canonical_payload(%Bill{} = bill) do
    [
      bill.uid,
      bill.rate_minor,
      bill.precharge_percent,
      bill.precharge_minor,
      bill.remainder_minor,
      bill.quota_debit
    ]
    |> Enum.map_join(&encode_field/1)
  end

  defp encode_field(value) when is_binary(value), do: <<byte_size(value)::32-big, value::binary>>

  defp encode_field(value) when is_integer(value) do
    encoded = Integer.to_string(value)
    <<byte_size(encoded)::32-big, encoded::binary>>
  end
end

defimpl Inspect, for: JasminEx.Billing.Fingerprint do
  def inspect(%JasminEx.Billing.Fingerprint{version: version}, _opts) do
    "#JasminEx.Billing.Fingerprint<version: #{version}, REDACTED>"
  end
end
