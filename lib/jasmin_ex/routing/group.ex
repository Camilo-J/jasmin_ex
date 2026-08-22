defmodule JasminEx.Routing.Group do
  @moduledoc false

  @gid_pattern ~r/^[A-Za-z0-9_-]{1,16}$/

  @enforce_keys [:gid, :enabled]
  defstruct @enforce_keys

  @type t :: %__MODULE__{gid: String.t(), enabled: boolean()}

  @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_gid | :invalid_enabled}
  def new(attrs) when is_list(attrs) do
    with {:ok, gid} <- validate_gid(Keyword.get(attrs, :gid)),
         {:ok, enabled} <- validate_enabled(Keyword.get(attrs, :enabled, true)) do
      {:ok, %__MODULE__{gid: gid, enabled: enabled}}
    end
  end

  def new(_attrs), do: {:error, :invalid_gid}

  @spec put_enabled(t(), boolean()) :: {:ok, t()} | {:error, :invalid_enabled}
  def put_enabled(%__MODULE__{} = group, enabled) do
    with {:ok, enabled} <- validate_enabled(enabled) do
      {:ok, %__MODULE__{group | enabled: enabled}}
    end
  end

  defp validate_gid(gid) when is_binary(gid) do
    if Regex.match?(@gid_pattern, gid), do: {:ok, gid}, else: {:error, :invalid_gid}
  end

  defp validate_gid(_gid), do: {:error, :invalid_gid}

  defp validate_enabled(enabled) when is_boolean(enabled), do: {:ok, enabled}
  defp validate_enabled(_enabled), do: {:error, :invalid_enabled}
end
