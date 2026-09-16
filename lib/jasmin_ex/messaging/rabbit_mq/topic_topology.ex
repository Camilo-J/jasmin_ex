defmodule JasminEx.Messaging.RabbitMQ.TopicTopology do
  @moduledoc false

  alias JasminEx.Messaging.RabbitMQ.Client

  @exchange "messaging"

  def names(prefix) when is_binary(prefix) do
    %{
      exchange: @exchange,
      dlx: prefix <> ".dlx",
      lookup: prefix <> ".lookup.v1",
      http: prefix <> ".http.v1",
      dead: prefix <> ".dead.v1"
    }
  end

  def lookup_arguments(dlx), do: retry_arguments(dlx, 10_000, 3)
  def http_arguments(dlx), do: retry_arguments(dlx, 30_000, 4)

  def dead_arguments do
    [
      {"x-queue-type", :longstr, "quorum"},
      {"x-overflow", :longstr, "reject-publish"}
    ]
  end

  def declare(channel, opts) when is_list(opts) do
    client = Keyword.get(opts, :client, Client)
    names = names(Keyword.fetch!(opts, :prefix))

    with :ok <- client.declare_exchange(channel, names.exchange, :topic, durable: true),
         :ok <- client.declare_exchange(channel, names.dlx, :fanout, durable: true),
         {:ok, _} <- retry_queue(client, channel, names.lookup, lookup_arguments(names.dlx)),
         {:ok, _} <- retry_queue(client, channel, names.http, http_arguments(names.dlx)),
         {:ok, _} <- client.declare_queue(channel, names.dead, queue_opts(dead_arguments())),
         :ok <- client.bind_queue(channel, names.lookup, names.exchange, routing_key: "dlr.*"),
         :ok <-
           client.bind_queue(channel, names.http, names.exchange, routing_key: "dlr_thrower.http") do
      client.bind_queue(channel, names.dead, names.dlx, routing_key: "")
    end
  end

  defp retry_arguments(dlx, delay_ms, delivery_limit) do
    [
      {"x-queue-type", :longstr, "quorum"},
      {"x-single-active-consumer", :bool, true},
      {"x-delayed-retry-type", :longstr, "all"},
      {"x-delayed-retry-min", :long, delay_ms},
      {"x-delayed-retry-max", :long, delay_ms},
      {"x-overflow", :longstr, "reject-publish"},
      {"x-dead-letter-exchange", :longstr, dlx},
      {"x-dead-letter-strategy", :longstr, "at-least-once"},
      {"x-delivery-limit", :long, delivery_limit}
    ]
  end

  defp retry_queue(client, channel, name, args) do
    case client.declare_queue(channel, name, queue_opts(args)) do
      {:ok, _} = ok -> ok
      {:error, _reason} -> {:error, :delayed_retry_unsupported}
    end
  end

  defp queue_opts(args), do: [durable: true, arguments: args]
end
