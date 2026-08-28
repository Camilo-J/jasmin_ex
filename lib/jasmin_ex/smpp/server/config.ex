defmodule JasminEx.Smpp.Server.Config do
  @moduledoc false

  defstruct enabled: false, host: {127, 0, 0, 1}, port: 2775, max_pdu_length: 65_536

  def new(opts \\ []) do
    %__MODULE__{
      enabled: Keyword.get(opts, :enabled, false),
      host: Keyword.get(opts, :host, {127, 0, 0, 1}),
      port: Keyword.get(opts, :port, 2775),
      max_pdu_length: Keyword.get(opts, :max_pdu_length, 65_536)
    }
  end
end
