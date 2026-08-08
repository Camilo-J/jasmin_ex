defmodule JasminEx.Messaging.WorkQueue do
  defmodule Delivery do
    @enforce_keys [:envelope, :reference]
    defstruct [:envelope, :reference]
  end

  @callback enqueue(term(), term()) :: :ok | {:error | :ambiguous, term()}
  @callback consume(term(), binary()) :: :ok | {:error, term()}
  @callback ack(term(), term()) :: :ok | {:error | :ambiguous, term()}
  @callback reject(term(), term(), term()) :: :ok | {:error | :ambiguous, term()}
  @callback retry(term(), term(), map()) :: :ok | {:error | :ambiguous, term()}
  @callback quarantine(term(), term(), map()) :: :ok | {:error | :ambiguous, term()}
  def enqueue(queue, envelope), do: dispatch(queue, :enqueue, [envelope])

  def consume(_queue, connector_id) when not is_binary(connector_id),
    do: {:error, {:invalid_argument, :connector_id}}

  def consume(queue, connector_id), do: dispatch(queue, :consume, [connector_id])

  def ack(queue, delivery) do
    with :ok <- validate_delivery(delivery), do: dispatch(queue, :ack, [delivery])
  end

  def reject(queue, delivery, reason) do
    with :ok <- validate_delivery(delivery), do: dispatch(queue, :reject, [delivery, reason])
  end

  def retry(queue, delivery, evidence) when is_map(evidence) do
    with :ok <- validate_delivery(delivery), do: dispatch(queue, :retry, [delivery, evidence])
  end

  def retry(_queue, _delivery, _evidence), do: {:error, {:invalid_argument, :evidence}}

  def quarantine(queue, delivery, evidence) when is_map(evidence) do
    with :ok <- validate_delivery(delivery),
         do: dispatch(queue, :quarantine, [delivery, evidence])
  end

  def quarantine(_queue, _delivery, _evidence), do: {:error, {:invalid_argument, :evidence}}

  defp validate_delivery(%Delivery{envelope: envelope, reference: reference})
       when not is_nil(envelope) and not is_nil(reference),
       do: :ok

  defp validate_delivery(_delivery), do: {:error, {:invalid_argument, :delivery}}

  defp dispatch({module, context}, operation, args),
    do: apply(module, operation, [context | args])
end
