defmodule JasminEx.Routing.Config do
  @moduledoc false

  alias JasminEx.Billing.Clock
  alias JasminEx.Billing.Clock.System, as: SystemClock

  @default_snapshot_path "var/jasmin_ex/routing-v1.json"

  defstruct snapshot_path: @default_snapshot_path, file_ops: nil, clock: SystemClock

  @type t :: %__MODULE__{
          snapshot_path: String.t(),
          file_ops: module() | nil,
          clock: Clock.clock()
        }

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      snapshot_path: Keyword.get(opts, :snapshot_path, @default_snapshot_path),
      file_ops: Keyword.get(opts, :file_ops),
      clock: Keyword.get(opts, :clock, SystemClock)
    }
  end
end
