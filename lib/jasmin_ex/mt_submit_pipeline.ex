defmodule JasminEx.MtSubmitPipeline do
  @moduledoc false

  alias JasminEx.MtSubmitPipeline.Production

  @order [:validate, :intercept, :route, :qos, :bill, :dispatch]
  @optional [:intercept, :qos]

  def submit(input, opts) when is_map(opts) do
    run(input, Production.stages(opts))
  end

  def run(input, stages) when is_map(stages) do
    Enum.reduce_while(@order, {:ok, input}, fn name, {:ok, current} ->
      case invoke(stages, name, current) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, reason} -> {:halt, {:error, {name, reason}}}
      end
    end)
  end

  defp invoke(stages, name, current) when name in @optional do
    Map.get(stages, name, &noop/1).(current)
  end

  defp invoke(stages, name, current) do
    case Map.fetch(stages, name) do
      {:ok, fun} -> fun.(current)
      :error -> {:error, :missing_port}
    end
  end

  defp noop(message), do: {:ok, message}
end
