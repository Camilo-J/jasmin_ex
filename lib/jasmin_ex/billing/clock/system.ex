defmodule JasminEx.Billing.Clock.System do
  @moduledoc false

  @behaviour JasminEx.Billing.Clock

  @spec wall_ms() :: integer()
  def wall_ms, do: wall_ms(nil)

  @impl true
  @spec wall_ms(term()) :: integer()
  def wall_ms(_state), do: System.system_time(:millisecond)

  @spec monotonic_ms() :: integer()
  def monotonic_ms, do: monotonic_ms(nil)

  @impl true
  @spec monotonic_ms(term()) :: integer()
  def monotonic_ms(_state), do: System.monotonic_time(:millisecond)
end
