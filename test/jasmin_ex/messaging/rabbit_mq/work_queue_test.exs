defmodule JasminEx.Messaging.RabbitMQ.WorkQueueTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.{Envelope, WorkQueue}
  alias JasminEx.Messaging.RabbitMQ.WorkQueue, as: Adapter
  alias JasminEx.Messaging.WorkQueue.Delivery
  alias JasminEx.Smpp.PDU.Tlv

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

  test "enqueue, retry, and quarantine preserve exact binary payloads" do
    message = <<0, 255, 0x1B, 0x14, 0xFF, 0xFE>>
    {queue, agent, envelope} = start_queue(FakePublisher, message)
    assert envelope.submit_sm.short_message === message
    assert :ok = WorkQueue.enqueue(queue, envelope)
    assert [{:publish, "alpha", payload}] = events(agent)
    wire = :json.decode(payload)
    assert wire["version"] === 2
    assert wire["submit_sm"]["short_message_base64"] == Base.encode64(message)
    assert {:ok, decoded} = Envelope.decode(payload)
    assert decoded.submit_sm.short_message === message
    assert decoded.attempt == 1

    delivery = %Delivery{envelope: envelope, reference: 7}
    assert :ok = WorkQueue.retry(queue, delivery, %{stage: :pre_write})
    assert :ok = WorkQueue.quarantine(queue, delivery, %{stage: :post_write, reason: :bind_lost})

    assert [
             {:publish, "alpha", _enqueued},
             {:publish, "alpha", retry_payload},
             {:ack, 7},
             {:publish, "alpha.quarantine", quarantine_payload},
             {:ack, 7}
           ] = events(agent)

    assert {:ok, retried} = Envelope.decode(retry_payload)
    assert retried.attempt == 2
    assert retried.max_attempts == envelope.max_attempts
    assert retried.gateway_id == envelope.gateway_id
    assert retried.connector_id == envelope.connector_id
    assert retried.enqueued_at == envelope.enqueued_at
    assert retried.expires_at == envelope.expires_at
    assert retried.submit_sm == envelope.submit_sm
    assert retried.submit_sm.short_message === message
    assert :json.decode(retry_payload)["version"] === 2

    assert {:ok, quarantined} = Envelope.decode(quarantine_payload)
    assert quarantined.submit_sm.short_message === message
    assert quarantined.attempt == 1
    assert quarantined.gateway_id == envelope.gateway_id

    assert :json.decode(quarantine_payload)["evidence"] == %{
             "reason" => "bind_lost",
             "stage" => "post_write"
           }
  end

  test "legacy v1 retry emits v2 while preserving bytes and metadata" do
    v1_payload =
      ~s({"version":1,"gateway_id":"gw-adapter","connector_id":"alpha","attempt":1,"max_attempts":3,"enqueued_at":"2026-08-01T15:00:00Z","expires_at":"2099-01-01T00:00:00Z","submit_sm":{"source_addr":"+12025550100","destination_addr":"+12025550101","short_message":"café"}})

    assert {:ok, envelope} = Envelope.decode(v1_payload)
    assert envelope.submit_sm.short_message === "café"
    {queue, agent, _fresh} = start_queue()
    delivery = %Delivery{envelope: envelope, reference: 3}
    assert :ok = WorkQueue.retry(queue, delivery, %{stage: :pre_write})
    assert [{:publish, "alpha", retry_payload}, {:ack, 3}] = events(agent)
    wire = :json.decode(retry_payload)
    assert wire["version"] === 2
    refute Map.has_key?(wire["submit_sm"], "short_message")
    assert wire["submit_sm"]["short_message_base64"] == Base.encode64("café")
    assert {:ok, retried} = Envelope.decode(retry_payload)
    assert retried.attempt == 2
    assert retried.submit_sm.short_message === "café"
    assert retried.gateway_id == "gw-adapter"
    assert retried.connector_id == "alpha"
    assert retried.max_attempts == 3
    assert retried.enqueued_at == "2026-08-01T15:00:00Z"
    assert retried.expires_at == "2099-01-01T00:00:00Z"
  end

  test "enqueue, retry, and quarantine preserve optional_parameters" do
    {:ok, optional} =
      Tlv.encode(
        sar_msg_ref_num: 42,
        sar_total_segments: 2,
        sar_segment_seqnum: 1
      )

    {queue, agent, envelope} = start_queue(FakePublisher, "hello", optional)
    assert envelope.submit_sm.optional_parameters === optional
    assert :ok = WorkQueue.enqueue(queue, envelope)
    assert [{:publish, "alpha", payload}] = events(agent)
    wire = :json.decode(payload)
    assert wire["version"] === 2
    refute Map.has_key?(wire["submit_sm"], "optional_parameters")
    assert wire["submit_sm"]["optional_parameters_base64"] == Base.encode64(optional)
    assert {:ok, decoded} = Envelope.decode(payload)
    assert decoded.submit_sm.optional_parameters === optional

    delivery = %Delivery{envelope: envelope, reference: 8}
    assert :ok = WorkQueue.retry(queue, delivery, %{stage: :pre_write})
    assert :ok = WorkQueue.quarantine(queue, delivery, %{stage: :post_write, reason: :bind_lost})

    assert [
             {:publish, "alpha", _enqueued},
             {:publish, "alpha", retry_payload},
             {:ack, 8},
             {:publish, "alpha.quarantine", quarantine_payload},
             {:ack, 8}
           ] = events(agent)

    assert {:ok, retried} = Envelope.decode(retry_payload)
    assert retried.attempt == 2
    assert retried.submit_sm.optional_parameters === optional
    assert retried.submit_sm == envelope.submit_sm
    assert :json.decode(retry_payload)["version"] === 2

    assert {:ok, quarantined} = Envelope.decode(quarantine_payload)
    assert quarantined.submit_sm.optional_parameters === optional
    assert quarantined.attempt == 1

    assert :json.decode(quarantine_payload)["evidence"] == %{
             "reason" => "bind_lost",
             "stage" => "post_write"
           }
  end

  defp start_queue(
         publisher \\ FakePublisher,
         short_message \\ "hello",
         optional_parameters \\ nil
       ) do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    {:ok, envelope} = valid_envelope(short_message, optional_parameters)
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

  defp valid_envelope(short_message, optional_parameters) do
    submit_sm = %{
      source_addr: "+12025550100",
      destination_addr: "+12025550101",
      short_message: short_message
    }

    submit_sm =
      if is_nil(optional_parameters),
        do: submit_sm,
        else: Map.put(submit_sm, :optional_parameters, optional_parameters)

    Envelope.new(%{
      gateway_id: "gw-adapter",
      connector_id: "alpha",
      attempt: 1,
      max_attempts: 3,
      enqueued_at: "2026-08-01T15:00:00Z",
      expires_at: "2099-01-01T00:00:00Z",
      submit_sm: submit_sm
    })
  end
end
