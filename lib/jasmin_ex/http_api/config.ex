defmodule JasminEx.HttpApi.Config do
  @moduledoc false

  defstruct enabled: false, host: {127, 0, 0, 1}, port: 1401

  def new(opts \\ []) do
    %__MODULE__{
      enabled: Keyword.get(opts, :enabled, false),
      host: Keyword.get(opts, :host, {127, 0, 0, 1}),
      port: Keyword.get(opts, :port, 1401)
    }
  end
end
