defmodule JasminEx.Messaging.RabbitMQ.WorkQueueTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.{Envelope, WorkQueue}
  alias JasminEx.Messaging.RabbitMQ.WorkQueue, as: Adapter
  alias JasminEx.Messaging.WorkQueue.Delivery

  defmodule FakePublisher do
    def publish(agent, connector_id, payload) do
      Agent.update(agent, &[{:publish, connector_id, payload} | &1])
      :ok
    end
  end

  defmodule FailingPublisher do
    def publish(agent, connector_id, payload) do
      Agent.update(agent, &[{:publish, connector_id, payload} | &1])
      {:error, :timeout}
    end
  end

  defmodule FakeClient do
    def consume(agent, queue, consumer, opts) do
      Agent.update(agent, &[{:consume, queue, consumer, opts} | &1])
      {:ok, "ctag-1"}
    end

    def ack(agent, tag) do
      Agent.update(agent, &[{:ack, tag} | &1])
      :ok
    end

    def reject(agent, tag, opts) do
      Agent.update(agent, &[{:reject, tag, opts} | &1])
      :ok
    end
  end

  test "enqueues an encoded envelope through the publisher without broker types" do
    {queue, agent, envelope} = start_queue()
    assert :ok = WorkQueue.enqueue(queue, envelope)
    assert [{:publish, "alpha", payload}] = events(agent)
    assert {:ok, ^envelope} = Envelope.decode(payload)
    refute inspect(queue) =~ "AMQP."
    refute inspect(payload) =~ "AMQP."
  end

  test "consumes, acks, and rejects through the client without broker types" do
    {queue, agent, envelope} = start_queue()
    delivery = %Delivery{envelope: envelope, reference: 9}
    assert :ok = WorkQueue.consume(queue, "alpha")
    assert :ok = WorkQueue.ack(queue, delivery)
    assert :ok = WorkQueue.reject(queue, delivery, :rejected)

    assert [
             {:consume, "jasmin.work.alpha", consumer, [no_ack: false]},
             {:ack, 9},
             {:reject, 9, [requeue: false]}
           ] = events(agent)

    assert consumer == self()
    refute inspect(delivery.reference) =~ "AMQP."
  end

  test "retries and quarantines through publisher then acks the source" do
    {queue, agent, envelope} = start_queue()
    delivery = %Delivery{envelope: envelope, reference: 4}
    assert :ok = WorkQueue.retry(queue, delivery, %{stage: :pre_write})
    assert :ok = WorkQueue.quarantine(queue, delivery, %{stage: :post_write, reason: :bind_lost})

    assert [
             {:publish, "alpha", retry_payload},
             {:ack, 4},
             {:publish, "alpha.quarantine", quarantine_payload},
             {:ack, 4}
           ] = events(agent)

    assert {:ok, retried} = Envelope.decode(retry_payload)
    assert retried.attempt == 2
    assert retried.gateway_id == envelope.gateway_id
    assert {:ok, ^envelope} = Envelope.decode(quarantine_payload)

    assert :json.decode(quarantine_payload)["evidence"] == %{
             "reason" => "bind_lost",
             "stage" => "post_write"
           }

    assert :ok = WorkQueue.quarantine(queue, %{delivery | reference: 9}, %{detail: "x"})
    assert :json.decode(elem(Enum.at(events(agent), 4), 2))["evidence"]["detail"] == "x"

    {_failing, fail_agent, same} = start_queue(FailingPublisher)
    fail_delivery = %Delivery{envelope: same, reference: 5}

    assert {:error, :timeout} =
             WorkQueue.retry({Adapter, context(fail_agent, FailingPublisher)}, fail_delivery, %{})

    assert [{:publish, "alpha", _}] = events(fail_agent)

    assert :ok = Adapter.republish(context(agent, FakePublisher), {:retry, envelope})
  end

  defp start_queue(publisher \\ FakePublisher) do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    {:ok, envelope} = valid_envelope()
    {{Adapter, context(agent, publisher)}, agent, envelope}
  end

  defp context(agent, publisher) do
    %{
      publisher: {publisher, agent},
      client: FakeClient,
      channel: agent,
      queue_prefix: "jasmin.work"
    }
  end

  defp events(agent), do: Agent.get(agent, &Enum.reverse/1)

  defp valid_envelope do
    Envelope.new(%{
      gateway_id: "gw-adapter",
      connector_id: "alpha",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2099-01-01T00:00:00Z",
      submit_sm: %{
        source_addr: "+12025550100",
        destination_addr: "+12025550101",
        short_message: "hello"
      }
    })
  end
end
