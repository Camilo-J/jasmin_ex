defmodule JasminEx.Messaging.RabbitMQ.Client do
  @moduledoc false

  def open_connection(opts) when is_list(opts) do
    case AMQP.Connection.open(opts) do
      {:ok, conn} -> {:ok, %{pid: conn.pid, conn: conn}}
      error -> error
    end
  end

  def close_connection(%{conn: conn}), do: AMQP.Connection.close(conn)
  def connection_pid(%{pid: pid}), do: pid

  def open_channel(%{conn: conn}) do
    case AMQP.Channel.open(conn) do
      {:ok, ch} -> {:ok, %{pid: ch.pid, channel: ch}}
      error -> error
    end
  end

  def close_channel(%{channel: ch}), do: AMQP.Channel.close(ch)
  def select_confirms(%{channel: ch}), do: AMQP.Confirm.select(ch)

  @classic_queue_arguments [{"x-queue-type", :longstr, "classic"}]

  def queue_declare_opts, do: [durable: true, arguments: @classic_queue_arguments]

  def declare_queue(%{channel: ch}, name, opts), do: AMQP.Queue.declare(ch, name, opts)

  def publish(%{channel: ch}, exchange, key, payload, opts),
    do: AMQP.Basic.publish(ch, exchange, key, payload, opts)

  def wait_for_confirms(%{channel: ch}, ms) when is_integer(ms) and ms > 0,
    do: AMQP.Confirm.wait_for_confirms(ch, {ms, :millisecond})

  def qos(%{channel: ch}, opts), do: AMQP.Basic.qos(ch, opts)

  def consume(%{channel: ch}, queue, consumer, opts),
    do: AMQP.Basic.consume(ch, queue, consumer, opts)

  def cancel(%{channel: ch}, consumer_tag), do: AMQP.Basic.cancel(ch, consumer_tag)
  def ack(%{channel: ch}, delivery_tag), do: AMQP.Basic.ack(ch, delivery_tag)

  def reject(%{channel: ch}, delivery_tag, opts) when is_list(opts),
    do: AMQP.Basic.reject(ch, delivery_tag, opts)
end
