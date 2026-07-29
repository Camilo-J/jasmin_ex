defmodule JasminEx.StateStore do
  @moduledoc """
  Public boundary for binary state-store operations.

  Callers provide binary keys and values, while operations dispatch through a
  `{module, context}` tuple that keeps store-specific details outside consumers.
  """

  @type store :: {module(), term()}
  @callback fetch(term(), binary()) :: {:ok, binary()} | :missing | {:error, term()}
  @callback put(term(), binary(), binary(), pos_integer()) :: :ok | {:error | :ambiguous, term()}
  @callback delete(term(), binary()) :: :deleted | :missing | {:error | :ambiguous, term()}

  def fetch(store, key) when is_binary(key), do: dispatch(store, :fetch, [key])
  def fetch(_store, _key), do: {:error, {:invalid_argument, :key}}

  def put(_store, key, _value, _ttl_ms) when not is_binary(key),
    do: {:error, {:invalid_argument, :key}}

  def put(_store, _key, value, _ttl_ms) when not is_binary(value),
    do: {:error, {:invalid_argument, :value}}

  def put(_store, _key, _value, ttl_ms) when not is_integer(ttl_ms) or ttl_ms <= 0,
    do: {:error, {:invalid_argument, :ttl_ms}}

  def put(store, key, value, ttl_ms), do: dispatch(store, :put, [key, value, ttl_ms])

  def delete(store, key) when is_binary(key), do: dispatch(store, :delete, [key])
  def delete(_store, _key), do: {:error, {:invalid_argument, :key}}

  defp dispatch({module, context}, operation, args),
    do: apply(module, operation, [context | args])
end
