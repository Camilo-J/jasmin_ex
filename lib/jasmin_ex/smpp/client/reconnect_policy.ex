defmodule JasminEx.Smpp.Client.ReconnectPolicy do
  @moduledoc false

  @default_base_ms 1_000
  @default_factor 2
  @default_cap_ms 30_000
  @default_jitter true

  @enforce_keys [:base_ms, :factor, :cap_ms, :jitter]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          base_ms: term(),
          factor: term(),
          cap_ms: term(),
          jitter: term()
        }

  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      base_ms: Keyword.get(opts, :reconnect_base_ms, @default_base_ms),
      factor: Keyword.get(opts, :reconnect_factor, @default_factor),
      cap_ms: Keyword.get(opts, :reconnect_cap_ms, @default_cap_ms),
      jitter: Keyword.get(opts, :reconnect_jitter, @default_jitter)
    }
  end

  @spec delay(t(), non_neg_integer(), (non_neg_integer() -> non_neg_integer())) ::
          non_neg_integer()
  def delay(%__MODULE__{} = policy, attempt, random) do
    delay = min(policy.base_ms * Integer.pow(policy.factor, attempt), policy.cap_ms)
    if policy.jitter, do: random.(delay), else: delay
  end
end
