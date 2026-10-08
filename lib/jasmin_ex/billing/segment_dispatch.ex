defmodule JasminEx.Billing.SegmentDispatch do
  @moduledoc false

  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Reservation
  alias JasminEx.Billing.SegmentLedger
  alias JasminEx.Billing.Tombstone

  defmodule Child do
    @moduledoc false

    @enforce_keys [:gateway_id, :payload_hash, :status]
    defstruct @enforce_keys

    @type status :: :unattempted | :claimed | :queued | :failed
    @type t :: %__MODULE__{gateway_id: binary(), payload_hash: binary(), status: status()}
  end

  @max_id_bytes 128
  @max_segment_count 255
  @known_keys [:bill_id, :fingerprint, :count, :children, :phase, :stop_outcome]
  @child_keys MapSet.new([
                :gateway_id,
                :payload_hash,
                :status,
                "gateway_id",
                "payload_hash",
                "status"
              ])
  @phases [:planned, :dispatching, :stopped, :closed]
  @statuses [:unattempted, :claimed, :queued, :failed]
  @stop_outcomes [:rejected, :uncertain]

  @enforce_keys [:bill_id, :fingerprint, :count, :children, :phase, :stop_outcome]
  defstruct @enforce_keys

  @type phase :: :planned | :dispatching | :stopped | :closed
  @type stop_outcome :: nil | :rejected | :uncertain
  @type recovery_action :: :refund | :hold_uncertain | :hold_queued | :reject

  @type t :: %__MODULE__{
          bill_id: binary(),
          fingerprint: Fingerprint.t(),
          count: 1..255,
          children: [Child.t()],
          phase: phase(),
          stop_outcome: stop_outcome()
        }

  @spec new(term()) ::
          {:ok, t()}
          | {:error,
             :invalid_bill_id
             | :invalid_fingerprint
             | :invalid_count
             | :invalid_gateway_id
             | :duplicate_gateway_id
             | :invalid_payload_hash
             | :invalid_dispatch
             | :unsafe_phase}
  def new(attrs) when is_list(attrs) do
    with :ok <- reject_unknown_keys(attrs),
         {:ok, bill_id} <- validate_id(Keyword.get(attrs, :bill_id)),
         {:ok, fingerprint} <- validate_fingerprint(Keyword.get(attrs, :fingerprint)),
         {:ok, count} <- validate_count(Keyword.get(attrs, :count)),
         {:ok, children} <- validate_children(Keyword.get(attrs, :children), bill_id, count),
         {:ok, phase} <- validate_phase(Keyword.get(attrs, :phase, :planned)),
         {:ok, stop_outcome} <-
           validate_stop_outcome(Keyword.get(attrs, :stop_outcome, nil), phase),
         :ok <- validate_phase_shape(phase, stop_outcome, children) do
      {:ok,
       %__MODULE__{
         bill_id: bill_id,
         fingerprint: fingerprint,
         count: count,
         children: children,
         phase: phase,
         stop_outcome: stop_outcome
       }}
    end
  end

  def new(_attrs), do: {:error, :invalid_dispatch}

  @spec plan(term(), term()) ::
          {:ok, t()}
          | {:error,
             :invalid_bill_id
             | :invalid_fingerprint
             | :invalid_count
             | :invalid_gateway_id
             | :duplicate_gateway_id
             | :invalid_payload_hash
             | :invalid_dispatch
             | :unsafe_phase}
  def plan(%Bill{} = bill, children) do
    {:ok, fingerprint} = Fingerprint.compute(bill)

    new(
      bill_id: bill.bill_id,
      fingerprint: fingerprint,
      count: bill.quota_debit,
      children: children
    )
  end

  def plan(_bill, _children), do: {:error, :invalid_dispatch}

  @spec bind(t(), Reservation.t() | Tombstone.t()) :: {:ok, t()} | {:error, :billing_conflict}
  def bind(%__MODULE__{} = dispatch, anchor) do
    if compatible?(dispatch, anchor), do: {:ok, dispatch}, else: {:error, :billing_conflict}
  end

  @spec compatible?(t(), Reservation.t() | Tombstone.t() | term()) :: boolean()
  def compatible?(
        %__MODULE__{bill_id: bill_id, fingerprint: fingerprint, count: count},
        %Reservation{
          bill_id: bill_id,
          fingerprint: fingerprint,
          ledger: %SegmentLedger{bill_id: bill_id, fingerprint: fingerprint, count: count}
        }
      ),
      do: true

  def compatible?(
        %__MODULE__{bill_id: bill_id, fingerprint: fingerprint},
        %Tombstone{bill_id: bill_id, fingerprint: fingerprint}
      ),
      do: true

  def compatible?(_dispatch, _anchor), do: false

  @spec same_bound_plan?(t(), t()) :: boolean()
  def same_bound_plan?(%__MODULE__{} = left, %__MODULE__{} = right) do
    left.bill_id == right.bill_id and left.fingerprint == right.fingerprint and
      left.count == right.count and bound_children(left) == bound_children(right)
  end

  def same_bound_plan?(_left, _right), do: false

  @spec claim(t(), term()) :: {:ok, t()} | {:error, atom()}
  def claim(%__MODULE__{phase: phase} = dispatch, gateway_id)
      when phase in [:planned, :dispatching] do
    with {:ok, index, child} <- fetch_child(dispatch, gateway_id),
         :ok <- ensure_claimable(dispatch, index, child) do
      children = List.replace_at(dispatch.children, index, %{child | status: :claimed})
      {:ok, %{dispatch | phase: :dispatching, children: children}}
    end
  end

  def claim(%__MODULE__{}, _gateway_id), do: {:error, :unsafe_phase}

  @spec confirm_queued(t(), term()) :: {:ok, t()} | {:error, atom()}
  def confirm_queued(%__MODULE__{phase: :dispatching} = dispatch, gateway_id) do
    case fetch_child(dispatch, gateway_id) do
      {:ok, index, %Child{status: :claimed} = child} ->
        children = List.replace_at(dispatch.children, index, %{child | status: :queued})
        {:ok, %{dispatch | children: children}}

      {:ok, _index, _child} ->
        {:error, :invalid_dispatch}

      error ->
        error
    end
  end

  def confirm_queued(%__MODULE__{}, _gateway_id), do: {:error, :unsafe_phase}

  @spec record_failure(t(), term()) :: {:ok, t()} | {:error, atom()}
  def record_failure(%__MODULE__{phase: :dispatching} = dispatch, gateway_id) do
    case fetch_child(dispatch, gateway_id) do
      {:ok, index, %Child{status: :claimed} = child} ->
        children = List.replace_at(dispatch.children, index, %{child | status: :failed})

        {:ok, %{dispatch | phase: :stopped, stop_outcome: :rejected, children: children}}

      {:ok, _index, _child} ->
        {:error, :invalid_dispatch}

      error ->
        error
    end
  end

  def record_failure(%__MODULE__{}, _gateway_id), do: {:error, :unsafe_phase}

  @spec close(t(), stop_outcome()) :: {:ok, t()} | {:error, atom()}
  def close(%__MODULE__{phase: :closed} = dispatch, _outcome), do: {:ok, dispatch}

  def close(%__MODULE__{} = dispatch, outcome) do
    new(
      bill_id: dispatch.bill_id,
      fingerprint: dispatch.fingerprint,
      count: dispatch.count,
      children:
        Enum.map(dispatch.children, fn child ->
          %{
            gateway_id: child.gateway_id,
            payload_hash: child.payload_hash,
            status: child.status
          }
        end),
      phase: :closed,
      stop_outcome: outcome
    )
  end

  def close(_dispatch, _outcome), do: {:error, :invalid_dispatch}

  @spec recovery_actions(t()) :: [%{gateway_id: binary(), action: recovery_action()}]
  def recovery_actions(%__MODULE__{children: children}) do
    Enum.map(children, fn child ->
      %{gateway_id: child.gateway_id, action: recovery_action(child.status)}
    end)
  end

  defp recovery_action(:unattempted), do: :refund
  defp recovery_action(:claimed), do: :hold_uncertain
  defp recovery_action(:queued), do: :hold_queued
  defp recovery_action(:failed), do: :reject

  defp reject_unknown_keys(attrs) do
    if Keyword.keys(attrs) -- @known_keys == [], do: :ok, else: {:error, :invalid_dispatch}
  end

  defp validate_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_id_bytes,
       do: {:ok, id}

  defp validate_id(_id), do: {:error, :invalid_bill_id}

  defp validate_fingerprint(%Fingerprint{version: 1, digest: digest} = fingerprint)
       when is_binary(digest) and byte_size(digest) == 32,
       do: {:ok, fingerprint}

  defp validate_fingerprint(_fingerprint), do: {:error, :invalid_fingerprint}

  defp validate_count(count)
       when is_integer(count) and count >= 1 and count <= @max_segment_count,
       do: {:ok, count}

  defp validate_count(_count), do: {:error, :invalid_count}

  defp validate_children(children, bill_id, count) when is_list(children) do
    if length(children) == count do
      reduce_children(children, bill_id)
    else
      {:error, :invalid_count}
    end
  end

  defp validate_children(_children, _bill_id, _count), do: {:error, :invalid_dispatch}

  defp reduce_children(children, bill_id) do
    Enum.reduce_while(children, {:ok, {[], MapSet.new()}}, fn item, {:ok, {acc, seen}} ->
      case decode_child_input(item, bill_id, seen) do
        {:ok, child} ->
          {:cont, {:ok, {[child | acc], MapSet.put(seen, child.gateway_id)}}}

        error ->
          {:halt, error}
      end
    end)
    |> finish_children()
  end

  defp finish_children({:ok, {acc, _seen}}), do: {:ok, Enum.reverse(acc)}
  defp finish_children(error), do: error

  defp decode_child_input(item, bill_id, seen) when is_map(item) do
    with :ok <- reject_unknown_child_keys(item),
         {:ok, gateway_id} <- validate_gateway_id(child_get(item, :gateway_id), bill_id, seen),
         {:ok, payload_hash} <- validate_payload_hash(child_get(item, :payload_hash)),
         {:ok, status} <- validate_status(child_get(item, :status) || :unattempted) do
      {:ok, %Child{gateway_id: gateway_id, payload_hash: payload_hash, status: status}}
    end
  end

  defp decode_child_input(_item, _bill_id, _seen), do: {:error, :invalid_dispatch}

  defp reject_unknown_child_keys(map) do
    if Enum.all?(Map.keys(map), &MapSet.member?(@child_keys, &1)) do
      :ok
    else
      {:error, :invalid_dispatch}
    end
  end

  defp child_get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp validate_gateway_id(id, bill_id, seen)
       when is_binary(id) and byte_size(id) > 0 and id != bill_id do
    if MapSet.member?(seen, id), do: {:error, :duplicate_gateway_id}, else: {:ok, id}
  end

  defp validate_gateway_id(_id, _bill_id, _seen), do: {:error, :invalid_gateway_id}

  defp validate_payload_hash(hash) when is_binary(hash) and byte_size(hash) == 32, do: {:ok, hash}
  defp validate_payload_hash(_hash), do: {:error, :invalid_payload_hash}

  defp validate_status(status) when status in @statuses, do: {:ok, status}
  defp validate_status(_status), do: {:error, :invalid_dispatch}

  defp validate_phase(phase) when phase in @phases, do: {:ok, phase}
  defp validate_phase(_phase), do: {:error, :unsafe_phase}

  defp validate_stop_outcome(nil, phase) when phase in [:planned, :dispatching, :closed],
    do: {:ok, nil}

  defp validate_stop_outcome(:rejected, :stopped), do: {:ok, :rejected}

  defp validate_stop_outcome(outcome, :closed) when outcome in @stop_outcomes, do: {:ok, outcome}

  defp validate_stop_outcome(_outcome, _phase), do: {:error, :unsafe_phase}

  defp validate_phase_shape(phase, stop_outcome, children) do
    statuses = Enum.map(children, & &1.status)

    if reachable_child_statuses?(phase, stop_outcome, statuses) do
      :ok
    else
      {:error, :unsafe_phase}
    end
  end

  defp reachable_child_statuses?(:planned, nil, statuses),
    do: Enum.all?(statuses, &(&1 == :unattempted))

  defp reachable_child_statuses?(:dispatching, nil, statuses),
    do: dispatching_sequence?(statuses)

  defp reachable_child_statuses?(:stopped, :rejected, statuses),
    do: stopped_failed_sequence?(statuses)

  defp reachable_child_statuses?(:closed, _outcome, statuses) do
    reachable_child_statuses?(:planned, nil, statuses) or
      reachable_child_statuses?(:dispatching, nil, statuses) or
      reachable_child_statuses?(:stopped, :rejected, statuses)
  end

  defp reachable_child_statuses?(_phase, _outcome, _statuses), do: false

  defp dispatching_sequence?(statuses) do
    {queued, rest} = Enum.split_while(statuses, &(&1 == :queued))

    case rest do
      [] ->
        queued != []

      [:claimed | suffix] ->
        Enum.all?(suffix, &(&1 == :unattempted))

      _other ->
        queued != [] and Enum.all?(rest, &(&1 == :unattempted))
    end
  end

  defp stopped_failed_sequence?(statuses) do
    {_queued, rest} = Enum.split_while(statuses, &(&1 == :queued))

    case rest do
      [:failed | suffix] -> Enum.all?(suffix, &(&1 == :unattempted))
      _other -> false
    end
  end

  defp bound_children(dispatch) do
    Enum.map(dispatch.children, &{&1.gateway_id, &1.payload_hash})
  end

  defp fetch_child(%__MODULE__{children: children}, gateway_id) do
    case Enum.find_index(children, &(&1.gateway_id == gateway_id)) do
      nil -> {:error, :invalid_gateway_id}
      index -> {:ok, index, Enum.at(children, index)}
    end
  end

  defp ensure_claimable(dispatch, index, %Child{status: :unattempted}) do
    prefix_queued? =
      dispatch.children
      |> Enum.take(index)
      |> Enum.all?(&(&1.status == :queued))

    if prefix_queued?, do: :ok, else: {:error, :invalid_dispatch}
  end

  defp ensure_claimable(_dispatch, _index, _child), do: {:error, :invalid_dispatch}
end

defimpl Inspect, for: JasminEx.Billing.SegmentDispatch do
  def inspect(
        %JasminEx.Billing.SegmentDispatch{bill_id: bill_id, count: count, phase: phase},
        _opts
      ) do
    "#JasminEx.Billing.SegmentDispatch<bill_id: #{inspect(bill_id)}, count: #{count}, phase: #{phase}, REDACTED>"
  end
end
