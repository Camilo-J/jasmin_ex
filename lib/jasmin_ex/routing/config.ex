defmodule JasminEx.Routing.Config do
  @moduledoc false

  @default_snapshot_path "var/jasmin_ex/routing-v1.json"

  defstruct snapshot_path: @default_snapshot_path, file_ops: nil

  @type t :: %__MODULE__{snapshot_path: String.t(), file_ops: module() | nil}

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      snapshot_path: Keyword.get(opts, :snapshot_path, @default_snapshot_path),
      file_ops: Keyword.get(opts, :file_ops)
    }
  end
end
