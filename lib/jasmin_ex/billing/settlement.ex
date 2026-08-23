defmodule JasminEx.Billing.Settlement do
  @moduledoc false

  alias JasminEx.Billing.Fingerprint

  @max_id_bytes 128
  @enforce_keys [:bill_id, :fingerprint, :outcome]
  defstruct @enforce_keys

  @type outcome :: :ok | :non_ok
  @type t :: %__MODULE__{bill_id: binary(), fingerprint: Fingerprint.t(), outcome: outcome()}

  @spec new(term()) :: {:ok, t()} | {:error, :invalid_bill_id}
  def new(attrs) when is_list(attrs) do
    with {:ok, bill_id} <- validate_id(Keyword.get(attrs, :bill_id)),
         {:ok, fingerprint} <- require_fingerprint(Keyword.get(attrs, :fingerprint)),
         {:ok, outcome} <- validate_outcome(Keyword.get(attrs, :outcome)) do
      {:ok, %__MODULE__{bill_id: bill_id, fingerprint: fingerprint, outcome: outcome}}
    end
  end

  def new(_attrs), do: {:error, :invalid_bill_id}

  @spec identity(t()) :: {binary(), Fingerprint.t(), outcome()}
  def identity(%__MODULE__{bill_id: bill_id, fingerprint: fingerprint, outcome: outcome}) do
    {bill_id, fingerprint, outcome}
  end

  defp validate_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_id_bytes,
       do: {:ok, id}

  defp validate_id(_id), do: {:error, :invalid_bill_id}

  defp require_fingerprint(%Fingerprint{version: 1, digest: digest} = fingerprint)
       when is_binary(digest) and byte_size(digest) == 32,
       do: {:ok, fingerprint}

  defp require_fingerprint(_fingerprint), do: {:error, :invalid_bill_id}

  defp validate_outcome(outcome) when outcome in [:ok, :non_ok], do: {:ok, outcome}
  defp validate_outcome(_outcome), do: {:error, :invalid_bill_id}
end
