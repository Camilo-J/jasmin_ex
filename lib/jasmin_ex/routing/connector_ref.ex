defmodule JasminEx.Routing.ConnectorRef do
  @moduledoc false

  @enforce_keys [:type, :id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{type: :smpp_client, id: String.t()}

  @spec new(term()) :: {:ok, t()} | {:error, :invalid_connector_id}
  def new(id) when is_binary(id) and id != "",
    do: {:ok, %__MODULE__{type: :smpp_client, id: id}}

  def new(_id), do: {:error, :invalid_connector_id}
end
