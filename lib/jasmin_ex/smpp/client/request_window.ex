defmodule JasminEx.Smpp.Client.RequestWindow do
  @moduledoc false

  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body

  @sequence_max 0x7FFF_FFFF

  defstruct next_sequence: 1, pending: %{}, cancellations: []

  @type entry :: %{from: term() | nil, command_id: atom()}
  @type reply_directive :: {term() | nil, term()}
  @type t :: %__MODULE__{
          next_sequence: pos_integer(),
          pending: %{optional(pos_integer()) => entry()},
          cancellations: [pos_integer()]
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec take_sequence(t()) :: {pos_integer(), t()}
  def take_sequence(%__MODULE__{next_sequence: sequence} = window) do
    {sequence, %{window | next_sequence: next_sequence(sequence)}}
  end

  @spec insert(t(), pos_integer(), atom(), term() | nil) :: t()
  def insert(%__MODULE__{} = window, sequence, command_id, from) do
    entry = %{from: from, command_id: command_id}
    %{window | pending: Map.put(window.pending, sequence, entry)}
  end

  @spec match(t(), pos_integer(), atom()) :: {:ok, entry()} | :error
  def match(%__MODULE__{} = window, sequence, command_id) do
    case Map.get(window.pending, sequence) do
      %{command_id: ^command_id} = entry -> {:ok, entry}
      _unmatched -> :error
    end
  end

  @spec resolve(t(), pos_integer(), atom()) :: {:ok, entry(), t()} | {:error, t()}
  def resolve(%__MODULE__{} = window, sequence, command_id) do
    case match(window, sequence, command_id) do
      {:ok, entry} ->
        {:ok, entry,
         %{
           window
           | pending: Map.delete(window.pending, sequence),
             cancellations: [sequence | window.cancellations]
         }}

      :error ->
        {:error, window}
    end
  end

  @spec expire(t(), pos_integer()) :: {entry() | nil, t()}
  def expire(%__MODULE__{} = window, sequence) do
    case Map.pop(window.pending, sequence) do
      {nil, _pending} -> {nil, window}
      {entry, pending} -> {entry, %{window | pending: pending}}
    end
  end

  @spec flush(t(), term()) :: {t(), [reply_directive()]}
  def flush(%__MODULE__{} = window, reply) do
    directives = Enum.map(window.pending, fn {_sequence, entry} -> {entry.from, reply} end)

    {%{
       window
       | pending: %{},
         cancellations: Map.keys(window.pending) ++ window.cancellations
     }, directives}
  end

  @spec flush_submits(t(), term()) :: {t(), [reply_directive()]}
  def flush_submits(%__MODULE__{} = window, reply) do
    {window, directives} =
      Enum.reduce(window.pending, {window, []}, fn
        {sequence, %{command_id: :submit_sm, from: from}}, {acc, directives} ->
          acc = remove_and_cancel(acc, sequence)
          {acc, [{from, reply} | directives]}

        {_sequence, _entry}, acc ->
          acc
      end)

    {window, Enum.reverse(directives)}
  end

  @spec drop_internal(t()) :: t()
  def drop_internal(%__MODULE__{} = window) do
    Enum.reduce(window.pending, window, fn
      {_sequence, %{command_id: :submit_sm}}, acc -> acc
      {sequence, _entry}, acc -> remove_and_cancel(acc, sequence)
    end)
  end

  @spec pending_submits?(t()) :: boolean()
  def pending_submits?(%__MODULE__{} = window) do
    Enum.any?(window.pending, fn {_sequence, entry} -> entry.command_id == :submit_sm end)
  end

  @spec pending_timeout_action(pos_integer(), non_neg_integer()) :: tuple()
  def pending_timeout_action(sequence, timeout),
    do: {{:timeout, {:pending, sequence}}, timeout, :pending_timeout}

  @spec drain_cancellations(t()) :: {[tuple()], t()}
  def drain_cancellations(%__MODULE__{} = window) do
    actions = Enum.map(window.cancellations, &{{:timeout, {:pending, &1}}, :cancel})
    {actions, %{window | cancellations: []}}
  end

  @spec classify_submit_response(PDU.t()) ::
          {:ok, String.t()} | {:error, {:submit_rejected, atom()}} | {:unknown, :invalid_response}
  def classify_submit_response(%PDU{status: status}) when status != :ESME_ROK,
    do: {:error, {:submit_rejected, status}}

  def classify_submit_response(%PDU{body: body}) do
    case Body.decode(:submit_sm_resp, body) do
      {:ok, %Body.SubmitSMResp{message_id: id}} when is_binary(id) and byte_size(id) > 0 ->
        {:ok, id}

      _invalid ->
        {:unknown, :invalid_response}
    end
  end

  defp next_sequence(sequence) when sequence >= @sequence_max, do: 1
  defp next_sequence(sequence), do: sequence + 1

  defp remove_and_cancel(window, sequence) do
    %{
      window
      | pending: Map.delete(window.pending, sequence),
        cancellations: [sequence | window.cancellations]
    }
  end
end
