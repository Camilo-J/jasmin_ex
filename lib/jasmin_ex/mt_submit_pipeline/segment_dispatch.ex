defmodule JasminEx.MtSubmitPipeline.SegmentDispatch do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Messaging.Envelope
  alias JasminEx.Messaging.WorkQueue
  alias JasminEx.Routing

  @spec run(GenServer.server(), term(), term(), term()) ::
          {:ok, binary()} | {:ok, :duplicate} | {:error, atom()}
  def run(router, queue, %Admission{bill: %Bill{} = bill} = admission, envelopes)
      when is_list(envelopes) do
    with {:ok, children} <- plan_children(bill, envelopes) do
      admit_and_publish(router, queue, admission, bill.bill_id, children, envelopes)
    end
  end

  def run(_router, _queue, _admission, _envelopes), do: {:error, :invalid_dispatch}

  defp admit_and_publish(router, queue, admission, bill_id, children, envelopes) do
    case call(fn -> Routing.admit_segments_with_dispatch(router, admission, children) end) do
      {:ok, _reservation, generation} ->
        publish_all(router, queue, bill_id, generation, envelopes)

      other ->
        other
    end
  end

  defp plan_children(%Bill{} = bill, envelopes) do
    with :ok <- validate_envelopes(bill, envelopes) do
      hash_children(envelopes)
    end
  end

  defp validate_envelopes(%Bill{} = bill, envelopes) do
    count = bill.quota_debit
    {:ok, fingerprint} = Fingerprint.compute(bill)

    if length(envelopes) == count do
      envelopes
      |> validate_indexed(bill, fingerprint, count)
      |> unique_gateway_ids(bill, envelopes)
    else
      {:error, :invalid_count}
    end
  end

  defp validate_indexed(envelopes, bill, fingerprint, count) do
    Enum.reduce_while(Enum.with_index(envelopes, 1), :ok, fn {envelope, index}, :ok ->
      halt_invalid(validate_envelope(bill, fingerprint, envelope, index, count))
    end)
  end

  defp halt_invalid(:ok), do: {:cont, :ok}
  defp halt_invalid(error), do: {:halt, error}

  defp validate_envelope(bill, fingerprint, %Envelope{segment: segment} = envelope, index, count)
       when is_map(segment) do
    digest = Base.encode64(fingerprint.digest)

    cond do
      segment.bill_id != bill.bill_id ->
        {:error, :invalid_dispatch}

      segment.count != count ->
        {:error, :invalid_count}

      segment.index != index ->
        {:error, :invalid_dispatch}

      segment.fingerprint_digest_base64 != digest ->
        {:error, :invalid_dispatch}

      true ->
        case Envelope.encode(envelope) do
          {:ok, _bytes} -> :ok
          error -> error
        end
    end
  end

  defp validate_envelope(_bill, _fingerprint, _envelope, _index, _count),
    do: {:error, :invalid_envelope}

  defp unique_gateway_ids(:ok, %Bill{bill_id: bill_id}, envelopes) do
    ids = Enum.map(envelopes, & &1.gateway_id)

    cond do
      Enum.any?(ids, &(&1 == bill_id)) -> {:error, :invalid_gateway_id}
      length(Enum.uniq(ids)) != length(ids) -> {:error, :duplicate_gateway_id}
      true -> :ok
    end
  end

  defp unique_gateway_ids(error, _bill, _envelopes), do: error

  defp hash_children(envelopes) do
    Enum.reduce_while(envelopes, {:ok, []}, fn envelope, {:ok, acc} ->
      case payload_hash(envelope) do
        {:ok, hash} ->
          {:cont, {:ok, [%{gateway_id: envelope.gateway_id, payload_hash: hash} | acc]}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, children} -> {:ok, Enum.reverse(children)}
      error -> error
    end
  end

  defp payload_hash(%Envelope{} = envelope) do
    case Envelope.encode(envelope) do
      {:ok, bytes} -> {:ok, :crypto.hash(:sha256, bytes)}
      error -> error
    end
  end

  defp publish_all(router, queue, bill_id, generation, envelopes) do
    Enum.reduce_while(envelopes, {:ok, bill_id}, fn envelope, _acc ->
      case publish_one(router, queue, bill_id, generation, envelope) do
        :ok -> {:cont, {:ok, bill_id}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp publish_one(router, queue, bill_id, generation, envelope) do
    call(fn ->
      with :ok <- verify_hash(router, bill_id, envelope),
           {:ok, _dispatch} <-
             Routing.claim_segment_dispatch(router, bill_id, generation, envelope.gateway_id),
           :ok <- ensure_router(router) do
        envelope
        |> enqueue(queue)
        |> record_enqueue(router, bill_id, generation, envelope.gateway_id)
      end
    end)
  end

  defp verify_hash(router, bill_id, envelope) do
    with {:ok, hash} <- payload_hash(envelope),
         %{payload_hash: ^hash} <- child_for(router, bill_id, envelope.gateway_id) do
      :ok
    else
      _other -> {:error, :invalid_payload_hash}
    end
  end

  defp child_for(router, bill_id, gateway_id) do
    case Routing.snapshot(router) do
      %{segment_dispatches: %{^bill_id => dispatch}} ->
        Enum.find(dispatch.children, &(&1.gateway_id == gateway_id))

      _other ->
        nil
    end
  end

  defp enqueue(envelope, queue) do
    WorkQueue.enqueue(queue, envelope)
  rescue
    error -> {:error, {:exception, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp record_enqueue(:ok, router, bill_id, generation, gateway_id) do
    case Routing.record_segment_dispatch(router, bill_id, generation, gateway_id, :queued) do
      {:ok, _result} -> :ok
      error -> error
    end
  end

  defp record_enqueue({:error, :non_ok}, router, bill_id, generation, gateway_id) do
    _ = Routing.record_segment_dispatch(router, bill_id, generation, gateway_id, :rejected)
    {:error, :non_ok}
  end

  defp record_enqueue(_other, router, bill_id, generation, gateway_id) do
    _ = Routing.record_segment_dispatch(router, bill_id, generation, gateway_id, :uncertain)
    {:error, :uncertain}
  end

  defp ensure_router(pid) when is_pid(pid) do
    if Process.alive?(pid), do: :ok, else: {:error, :router_unavailable}
  end

  defp ensure_router(name) when is_atom(name) do
    if is_pid(Process.whereis(name)), do: :ok, else: {:error, :router_unavailable}
  end

  defp ensure_router(_server), do: :ok

  defp call(fun) do
    fun.()
  catch
    :exit, _reason -> {:error, :router_unavailable}
  end
end
