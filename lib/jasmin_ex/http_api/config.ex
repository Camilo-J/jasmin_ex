defmodule JasminEx.HttpApi.Config do
  @moduledoc false

  defstruct enabled: false,
            host: {127, 0, 0, 1},
            port: 1401,
            concat: :udh,
            max_segments: 5

  def new(opts \\ [])

  def new(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "HTTP API configuration must be a keyword list"
    end

    %__MODULE__{
      enabled: Keyword.get(opts, :enabled, false),
      host: Keyword.get(opts, :host, {127, 0, 0, 1}),
      port: Keyword.get(opts, :port, 1401),
      concat: concat!(Keyword.get(opts, :concat, :udh)),
      max_segments: max_segments!(Keyword.get(opts, :max_segments, 5))
    }
  end

  def new(_opts), do: raise(ArgumentError, "HTTP API configuration must be a keyword list")

  defp concat!(concat) when concat in [:udh, :sar], do: concat
  defp concat!(_concat), do: raise(ArgumentError, "HTTP API concat must be :udh or :sar")

  defp max_segments!(max_segments) when is_integer(max_segments) and max_segments in 1..5,
    do: max_segments

  defp max_segments!(_max_segments),
    do: raise(ArgumentError, "HTTP API max_segments must be an integer from 1 to 5")
end
