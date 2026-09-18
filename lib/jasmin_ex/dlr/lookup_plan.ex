defmodule JasminEx.Dlr.LookupPlan do
  @moduledoc false

  alias JasminEx.Dlr.{Event, Lookup}
  alias JasminEx.Dlr.Map, as: DlrMap
  alias JasminEx.StateStore

  @phases [:planned, :forwarded, :complete]

  @spec key(binary()) :: binary()
  def key(event_id) when is_binary(event_id) do
    <<"DLR", 1, "plan", byte_size(event_id)::32, event_id::binary>>
  end

  @spec process(binary(), map(), keyword()) :: :ok | :retry | :terminal
  def process(payload, _meta, context) when is_binary(payload) do
    case Event.decode(payload) do
      {:ok, event} -> process_event(event, context)
      {:error, _reason} -> :terminal
    end
  end

  @spec process_event(map(), keyword()) :: :ok | :retry | :terminal
  def process_event(event, context) when is_map(event) and is_list(context) do
    store = Keyword.fetch!(context, :store)
    clock = Keyword.fetch!(context, :clock)

    case fetch(store, event.event_id, clock) do
      {:ok, plan} -> execute(plan, context)
      :missing -> create_and_execute(event, context)
      {:error, :expired} -> :terminal
      {:error, {:malformed_plan, _reason}} -> :terminal
      {:error, _reason} -> :retry
    end
  end

  @spec persist(StateStore.store(), map(), {module(), term()}) ::
          :ok | {:error | :ambiguous, term()}
  def persist(store, plan, clock) do
    with {:ok, payload} <- encode(plan) do
      StateStore.put(store, key(plan.event_id), payload, ttl(plan, now_ms(clock)))
    end
  end

  @spec fetch(StateStore.store(), binary(), {module(), term()}) ::
          {:ok, map()} | :missing | {:error, term()}
  def fetch(store, event_id, clock) do
    case StateStore.fetch(store, key(event_id)) do
      :missing -> :missing
      {:error, _reason} = error -> error
      {:ok, payload} -> decode_fresh(payload, now_ms(clock))
    end
  end

  @spec encode(map()) :: {:ok, binary()} | {:error, :invalid_plan}
  def encode(plan) when is_map(plan) do
    with :ok <- validate(plan) do
      payload = %{
        "version" => 1,
        "event_id" => plan.event_id,
        "phase" => Atom.to_string(plan.phase),
        "expires_at_ms" => plan.expires_at_ms,
        "job" => encode_job(plan.job),
        "reverse" => encode_reverse(plan.reverse),
        "cleanup" => encode_cleanup(plan.cleanup)
      }

      {:ok, payload |> :json.encode() |> IO.iodata_to_binary()}
    end
  rescue
    _error -> {:error, :invalid_plan}
  end

  def encode(_plan), do: {:error, :invalid_plan}

  @spec decode(binary()) :: {:ok, map()} | {:error, atom()}
  def decode(payload) when is_binary(payload) do
    case :json.decode(payload) do
      %{"version" => 1} = map -> decode_v1(map)
      %{"version" => _version} -> {:error, :unsupported_version}
      _other -> {:error, :invalid_plan}
    end
  rescue
    _error -> {:error, :invalid_plan}
  end

  defp create_and_execute(event, context) do
    with {:ok, plan} <- build(event, context),
         :ok <- persist(Keyword.fetch!(context, :store), plan, Keyword.fetch!(context, :clock)) do
      execute(plan, context)
    else
      {:retry, _reason} -> :retry
      {:terminal, _reason} -> :terminal
      {:error, {:malformed_plan, _reason}} -> :terminal
      {:error, _reason} -> :retry
      {:ambiguous, _reason} -> :retry
    end
  end

  defp build(%{kind: :submit_sm_resp} = event, context) do
    inputs = [request: DlrMap.fetch_request(store(context), event.gateway_id, clock(context))]
    to_plan(event, inputs, context)
  end

  defp build(%{kind: :deliver_sm} = event, context) do
    reverse =
      DlrMap.fetch_reverse(store(context), event.connector_id, event.raw_smsc_id, clock(context))

    case reverse do
      {:ok, record} ->
        request = DlrMap.fetch_request(store(context), record.gateway_id, clock(context))
        to_plan(event, [reverse: reverse, request: request], context)

      other ->
        to_plan(event, [reverse: other], context)
    end
  end

  defp build(_event, _context), do: {:terminal, :unknown_event}

  defp to_plan(event, inputs, context) do
    case Lookup.plan(event, Keyword.put(inputs, :now_ms, now_ms(clock(context)))) do
      {:ok, actions} ->
        {:ok,
         Map.merge(actions, %{
           version: 1,
           phase: :planned
         })}

      other ->
        other
    end
  end

  defp execute(%{phase: :complete}, _context), do: :ok

  defp execute(%{phase: :planned} = plan, context) do
    case apply_reverse(plan.reverse, context) do
      :ok -> publish_and_advance(plan, context)
      {:terminal, _reason} -> :terminal
      :retry -> :retry
    end
  end

  defp execute(%{phase: :forwarded} = plan, context) do
    with :ok <- cleanup(plan.cleanup, context),
         {:ok, _complete} <- mark(plan, :complete, context) do
      :ok
    else
      :retry -> :retry
    end
  end

  defp publish_and_advance(plan, context) do
    with :ok <- publish(plan.job, context),
         {:ok, forwarded} <- mark(plan, :forwarded, context) do
      execute(forwarded, context)
    else
      :retry -> :retry
    end
  end

  defp apply_reverse(nil, _context), do: :ok

  defp apply_reverse(reverse, context) do
    case DlrMap.put_reverse(store(context), reverse, clock(context)) do
      :ok -> :ok
      {:error, {:malformed_map, _reason} = reason} -> {:terminal, reason}
      {:error, :reverse_collision} -> {:terminal, :reverse_collision}
      _other -> :retry
    end
  end

  defp publish(nil, _context), do: :ok

  defp publish(job, context) do
    case Keyword.fetch!(context, :publisher).(job) do
      :ok -> :ok
      _other -> :retry
    end
  end

  defp cleanup(nil, _context), do: :ok

  defp cleanup({:request, gateway_id}, context) do
    case DlrMap.delete_request(store(context), gateway_id) do
      result when result in [:deleted, :missing] -> :ok
      _other -> :retry
    end
  end

  defp mark(plan, phase, context) do
    updated = %{plan | phase: phase}

    case persist(store(context), updated, clock(context)) do
      :ok -> {:ok, updated}
      _other -> :retry
    end
  end

  defp validate(%{
         version: 1,
         event_id: event_id,
         phase: phase,
         expires_at_ms: expires_at_ms,
         job: job,
         reverse: reverse,
         cleanup: cleanup
       })
       when is_binary(event_id) and event_id != "" and phase in @phases and
              is_integer(expires_at_ms) do
    with :ok <- validate_job(job),
         :ok <- validate_reverse(reverse),
         do: validate_cleanup(cleanup)
  end

  defp validate(_plan), do: {:error, :invalid_plan}

  defp validate_job(nil), do: :ok

  defp validate_job(job) when is_map(job) do
    required = [
      :job_id,
      :event_id,
      :gateway_id,
      :url,
      :method,
      :level,
      :created_at_ms,
      :deadline_ms,
      :fields
    ]

    if Enum.all?(required, &Map.has_key?(job, &1)) and job.method in ["GET", "POST"] and
         job.level in [1, 2] and is_map(job.fields),
       do: :ok,
       else: {:error, :invalid_plan}
  end

  defp validate_job(_job), do: {:error, :invalid_plan}

  defp validate_reverse(nil), do: :ok

  defp validate_reverse(reverse) when is_map(reverse) do
    if Enum.all?(
         [:connector_id, :raw_smsc_id, :gateway_id, :expiry_s],
         &Map.has_key?(reverse, &1)
       ),
       do: :ok,
       else: {:error, :invalid_plan}
  end

  defp validate_reverse(_reverse), do: {:error, :invalid_plan}
  defp validate_cleanup(nil), do: :ok
  defp validate_cleanup({:request, id}) when is_binary(id) and id != "", do: :ok
  defp validate_cleanup(_cleanup), do: {:error, :invalid_plan}

  defp encode_job(nil), do: :null

  defp encode_job(job) do
    %{
      "job_id" => job.job_id,
      "event_id" => job.event_id,
      "gateway_id" => job.gateway_id,
      "url" => job.url,
      "method" => job.method,
      "level" => job.level,
      "created_at_ms" => job.created_at_ms,
      "deadline_ms" => job.deadline_ms,
      "fields" => job.fields
    }
  end

  defp encode_reverse(nil), do: :null

  defp encode_reverse(reverse) do
    %{
      "connector_id" => reverse.connector_id,
      "raw_smsc_id" => reverse.raw_smsc_id,
      "gateway_id" => reverse.gateway_id,
      "expiry_s" => reverse.expiry_s
    }
  end

  defp encode_cleanup(nil), do: :null
  defp encode_cleanup({:request, id}), do: %{"kind" => "request", "gateway_id" => id}

  defp decode_v1(map) do
    with {:ok, phase} <- decode_phase(map["phase"]),
         {:ok, job} <- decode_job(map["job"]),
         {:ok, reverse} <- decode_reverse(map["reverse"]),
         {:ok, cleanup} <- decode_cleanup(map["cleanup"]),
         event_id when is_binary(event_id) and event_id != "" <- map["event_id"],
         expires_at_ms when is_integer(expires_at_ms) <- map["expires_at_ms"] do
      plan = %{
        version: 1,
        event_id: event_id,
        phase: phase,
        expires_at_ms: expires_at_ms,
        job: job,
        reverse: reverse,
        cleanup: cleanup
      }

      with :ok <- validate(plan), do: {:ok, plan}
    else
      _other -> {:error, :invalid_plan}
    end
  end

  defp decode_phase("planned"), do: {:ok, :planned}
  defp decode_phase("forwarded"), do: {:ok, :forwarded}
  defp decode_phase("complete"), do: {:ok, :complete}
  defp decode_phase(_phase), do: {:error, :invalid_plan}

  defp decode_job(:null), do: {:ok, nil}

  defp decode_job(map) when is_map(map) do
    job = %{
      job_id: map["job_id"],
      event_id: map["event_id"],
      gateway_id: map["gateway_id"],
      url: map["url"],
      method: map["method"],
      level: map["level"],
      created_at_ms: map["created_at_ms"],
      deadline_ms: map["deadline_ms"],
      fields: map["fields"]
    }

    with :ok <- validate_job(job), do: {:ok, job}
  end

  defp decode_job(_map), do: {:error, :invalid_plan}

  defp decode_reverse(:null), do: {:ok, nil}

  defp decode_reverse(map) when is_map(map) do
    reverse = %{
      connector_id: map["connector_id"],
      raw_smsc_id: map["raw_smsc_id"],
      gateway_id: map["gateway_id"],
      expiry_s: map["expiry_s"]
    }

    with :ok <- validate_reverse(reverse), do: {:ok, reverse}
  end

  defp decode_reverse(_map), do: {:error, :invalid_plan}

  defp decode_cleanup(:null), do: {:ok, nil}

  defp decode_cleanup(%{"kind" => "request", "gateway_id" => id})
       when is_binary(id) and id != "",
       do: {:ok, {:request, id}}

  defp decode_cleanup(_cleanup), do: {:error, :invalid_plan}

  defp decode_fresh(payload, now_ms) do
    case decode(payload) do
      {:ok, %{expires_at_ms: expires_at_ms}} when expires_at_ms <= now_ms -> {:error, :expired}
      {:ok, plan} -> {:ok, plan}
      {:error, reason} -> {:error, {:malformed_plan, reason}}
    end
  end

  defp store(context), do: Keyword.fetch!(context, :store)
  defp clock(context), do: Keyword.fetch!(context, :clock)
  defp now_ms({module, value}), do: module.now_ms(value)
  defp ttl(plan, now_ms), do: max(plan.expires_at_ms - now_ms, 1)
end
