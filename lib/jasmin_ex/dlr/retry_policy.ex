defmodule JasminEx.Dlr.RetryPolicy do
  @moduledoc false

  @additional %{lookup: 2, http: 3}
  @delay_ms %{lookup: 10_000, http: 30_000}

  def additional_attempts(kind) when is_map_key(@additional, kind), do: @additional[kind]
  def delay_ms(kind) when is_map_key(@delay_ms, kind), do: @delay_ms[kind]
  def total_attempts(kind), do: additional_attempts(kind) + 1

  def failures(kind, meta) do
    with {:ok, delivery} <- delivery_count(meta),
         {:ok, acquired} <- acquired_count(meta) do
      if acquired > total_attempts(kind) * 10 do
        {:error, :exhausted}
      else
        {:ok, delivery}
      end
    end
  end

  def settle(kind, meta, :retry) do
    case failures(kind, meta) do
      {:ok, count} ->
        if count < additional_attempts(kind),
          do: {:reject, requeue: true},
          else: {:reject, requeue: false}

      _ ->
        {:reject, requeue: false}
    end
  end

  def settle(_kind, _meta, :ok), do: :ack
  def settle(_kind, _meta, :terminal), do: {:reject, requeue: false}

  defp delivery_count(meta) do
    typed_count(meta, "x-delivery-count", required_on_redelivery?(meta))
  end

  defp acquired_count(meta) do
    typed_count(meta, "x-acquired-count", false)
  end

  defp typed_count(meta, name, required?) do
    case header(meta, name) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      :missing when required? -> {:error, :malformed_counters}
      :missing -> {:ok, 0}
      :malformed -> {:error, :malformed_counters}
    end
  end

  defp required_on_redelivery?(%{redelivered: true}), do: true
  defp required_on_redelivery?(_meta), do: false

  defp header(meta, name) do
    case Map.get(meta, :headers) do
      headers when is_list(headers) ->
        case List.keyfind(headers, name, 0) do
          {^name, :long, value} when is_integer(value) -> {:ok, value}
          {^name, _type, _value} -> :malformed
          nil -> :missing
        end

      _ ->
        :missing
    end
  end
end
