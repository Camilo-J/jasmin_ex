defmodule JasminEx.Messaging.RabbitMQ.WorkQueue do
  @moduledoc false

  alias JasminEx.Messaging.Envelope
  alias JasminEx.Messaging.RabbitMQ.Publisher
  alias JasminEx.Messaging.WorkQueue.Delivery

  def enqueue(context, envelope) do
    with {:ok, payload} <- Envelope.encode(envelope) do
      publish(context, envelope.connector_id, payload)
    end
  end

  def consume(context, connector_id) do
    case client(context).consume(
           channel(context),
           queue(context, connector_id),
           self(),
           no_ack: false
         ) do
      {:ok, _tag} -> :ok
      error -> error
    end
  end

  def ack(context, %Delivery{reference: tag}), do: client(context).ack(channel(context), tag)

  def reject(context, %Delivery{reference: tag}, _reason),
    do: client(context).reject(channel(context), tag, requeue: false)

  def retry(context, %Delivery{envelope: envelope, reference: tag}, _evidence) do
    with {:ok, next} <- increment(envelope),
         :ok <- delayed_publish(context, next) do
      client(context).ack(channel(context), tag)
    end
  end

  def quarantine(context, %Delivery{envelope: envelope, reference: tag}, evidence) do
    with {:ok, payload} <- Envelope.encode(envelope),
         :ok <-
           publish(context, envelope.connector_id <> ".quarantine", with_ev(payload, evidence)) do
      client(context).ack(channel(context), tag)
    end
  end

  def republish(action), do: republish(%{publisher: Publisher}, action)
  def republish(context, {:retry, envelope}), do: delayed_publish(context, envelope)

  def republish(context, {:quarantine, envelope, evidence}) do
    with {:ok, payload} <- Envelope.encode(envelope) do
      publish(context, envelope.connector_id <> ".quarantine", with_ev(payload, evidence))
    end
  end

  defp delayed_publish(context, envelope) do
    with {:ok, payload} <- Envelope.encode(envelope) do
      publish_retry(context, envelope.connector_id, payload)
    end
  end

  defp publish(%{publisher: {module, server}}, connector_id, payload),
    do: module.publish(server, connector_id, payload)

  defp publish(%{publisher: server}, connector_id, payload),
    do: Publisher.publish(server, connector_id, payload)

  defp publish_retry(%{publisher: {module, server}}, connector_id, payload),
    do: module.publish_retry(server, connector_id, payload)

  defp publish_retry(%{publisher: server}, connector_id, payload),
    do: Publisher.publish_retry(server, connector_id, payload)

  defp client(%{client: client}), do: client
  defp channel(%{channel: channel}), do: channel
  defp queue(%{queue_prefix: prefix}, connector_id), do: prefix <> "." <> connector_id

  defp with_ev(payload, evidence) do
    ev = Map.new(evidence, fn {key, value} -> {to_string(key), to_string(value)} end)

    payload
    |> :json.decode()
    |> Map.put("evidence", ev)
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp increment(envelope), do: Envelope.retry(envelope)
end
