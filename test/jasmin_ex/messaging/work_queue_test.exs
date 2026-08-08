defmodule JasminEx.Messaging.WorkQueueTest do
  use ExUnit.Case, async: true
  alias JasminEx.Messaging.WorkQueue
  alias JasminEx.Messaging.WorkQueue.Delivery

  defmodule Queue do
    def enqueue(context, envelope), do: send(context, {:enqueue, envelope})
    def consume(context, connector_id), do: send(context, {:consume, connector_id})
    def ack(context, delivery), do: send(context, {:ack, delivery})
    def reject(context, delivery, reason), do: send(context, {:reject, delivery, reason})
    def retry(context, delivery, evidence), do: send(context, {:retry, delivery, evidence})

    def quarantine(context, delivery, evidence),
      do: send(context, {:quarantine, delivery, evidence})
  end

  test "dispatches semantic queue operations through an opaque adapter context" do
    queue = {Queue, self()}
    envelope = :submission_envelope
    delivery = %Delivery{envelope: envelope, reference: make_ref()}
    assert WorkQueue.enqueue(queue, envelope) == {:enqueue, envelope}
    assert WorkQueue.consume(queue, "connector-a") == {:consume, "connector-a"}
    assert WorkQueue.ack(queue, delivery) == {:ack, delivery}
    assert WorkQueue.reject(queue, delivery, :rejected) == {:reject, delivery, :rejected}

    assert WorkQueue.retry(queue, delivery, %{stage: :pre_write}) ==
             {:retry, delivery, %{stage: :pre_write}}

    assert WorkQueue.quarantine(queue, delivery, %{stage: :post_write}) ==
             {:quarantine, delivery, %{stage: :post_write}}
  end

  test "rejects invalid public contract arguments before dispatch" do
    queue = {Queue, self()}
    assert WorkQueue.consume(queue, :connector) == {:error, {:invalid_argument, :connector_id}}
    assert WorkQueue.ack(queue, :delivery) == {:error, {:invalid_argument, :delivery}}

    assert WorkQueue.retry(queue, :delivery, :evidence) ==
             {:error, {:invalid_argument, :evidence}}

    refute_received _message
  end
end
