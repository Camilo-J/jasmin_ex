defmodule JasminEx.Billing.FakeClock do
  @moduledoc false

  @behaviour JasminEx.Billing.Clock

  @enforce_keys [:wall_ms, :monotonic_ms]
  defstruct @enforce_keys

  @type t :: %__MODULE__{wall_ms: integer(), monotonic_ms: integer()}

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      wall_ms: Keyword.get(opts, :wall_ms, 0),
      monotonic_ms: Keyword.get(opts, :monotonic_ms, 0)
    }
  end

  @impl true
  def wall_ms(%__MODULE__{wall_ms: wall_ms}), do: wall_ms

  @impl true
  def monotonic_ms(%__MODULE__{monotonic_ms: monotonic_ms}), do: monotonic_ms

  @spec advance(t(), integer()) :: t()
  def advance(%__MODULE__{} = clock, by_ms) when is_integer(by_ms) do
    %__MODULE__{
      wall_ms: clock.wall_ms + by_ms,
      monotonic_ms: clock.monotonic_ms + by_ms
    }
  end
end
