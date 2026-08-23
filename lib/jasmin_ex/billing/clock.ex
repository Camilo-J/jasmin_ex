defmodule JasminEx.Billing.Clock do
  @moduledoc false

  @type clock :: module() | {module(), term()}

  @callback wall_ms(term()) :: integer()
  @callback monotonic_ms(term()) :: integer()

  @spec wall_ms(clock()) :: integer()
  def wall_ms({module, state}), do: module.wall_ms(state)
  def wall_ms(module) when is_atom(module), do: module.wall_ms(nil)

  @spec monotonic_ms(clock()) :: integer()
  def monotonic_ms({module, state}), do: module.monotonic_ms(state)
  def monotonic_ms(module) when is_atom(module), do: module.monotonic_ms(nil)
end
