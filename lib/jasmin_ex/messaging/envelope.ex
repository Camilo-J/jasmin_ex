defmodule JasminEx.Messaging.Envelope do
  @moduledoc "Represents and serializes a queued messaging request."

  @version 1
  @fields [
    :gateway_id,
    :connector_id,
    :attempt,
    :max_attempts,
    :enqueued_at,
    :expires_at,
    :submit_sm
  ]
  @enforce_keys @fields
  defstruct @fields
  @allowed_data_coding [0, 1, 2, 3, 8]

  def new(attributes) when is_map(attributes) do
    with :ok <- reject_unknown_keys(attributes),
         {:ok, submit_sm} <- validate_submit_sm(Map.get(attributes, :submit_sm)),
         true <- valid_attributes?(attributes) do
      {:ok, struct!(__MODULE__, Map.put(attributes, :submit_sm, submit_sm))}
    else
      _ -> {:error, :invalid_envelope}
    end
  end

  def new(_attributes), do: {:error, :invalid_envelope}

  def encode(%__MODULE__{} = envelope) do
    payload =
      envelope
      |> Map.from_struct()
      |> Map.put(:version, @version)
      |> stringify_envelope()
      |> :json.encode()
      |> IO.iodata_to_binary()

    {:ok, payload}
  end

  def decode(payload) when is_binary(payload) do
    with {:ok, attributes} <- decode_json(payload),
         :ok <- validate_version(attributes) do
      attributes
      |> Map.drop(["version"])
      |> atomize_known_keys()
      |> new()
    else
      {:error, :unsupported_version} -> {:error, :unsupported_version}
      _ -> {:error, :invalid_envelope}
    end
  end

  def decode(_payload), do: {:error, :invalid_envelope}

  defp decode_json(payload) do
    {:ok, :json.decode(payload)}
  rescue
    _error -> {:error, :invalid_json}
  end

  defp validate_version(%{"version" => @version}), do: :ok
  defp validate_version(%{"version" => _version}), do: {:error, :unsupported_version}
  defp validate_version(_attributes), do: {:error, :invalid_envelope}

  defp reject_unknown_keys(attributes) do
    if Map.keys(attributes) -- @fields == [], do: :ok, else: :error
  end

  defp valid_attributes?(attributes) do
    Enum.all?(
      [:gateway_id, :connector_id, :enqueued_at, :expires_at],
      &is_binary(Map.get(attributes, &1))
    ) and
      Enum.all?(
        [:attempt, :max_attempts],
        &(is_integer(Map.get(attributes, &1)) and Map.get(attributes, &1) > 0)
      ) and
      Map.get(attributes, :attempt) <= Map.get(attributes, :max_attempts)
  end

  defp validate_submit_sm(
         %{
           "source_addr" => source,
           "destination_addr" => destination,
           "short_message" => message
         } = submit_sm
       ),
       do:
         validate_submit_sm(%{
           source_addr: source,
           destination_addr: destination,
           short_message: message,
           data_coding: Map.get(submit_sm, "data_coding")
         })

  defp validate_submit_sm(
         %{
           source_addr: source,
           destination_addr: destination,
           short_message: message
         } = submit_sm
       )
       when is_binary(source) and is_binary(destination) and is_binary(message) do
    case normalize_data_coding(Map.get(submit_sm, :data_coding)) do
      {:ok, data_coding} ->
        {:ok,
         %{
           source_addr: source,
           destination_addr: destination,
           short_message: message,
           data_coding: data_coding
         }}

      :error ->
        {:error, :invalid_submit_sm}
    end
  end

  defp validate_submit_sm(_submit_sm), do: {:error, :invalid_submit_sm}

  defp normalize_data_coding(nil), do: {:ok, 0}

  defp normalize_data_coding(data_coding) when data_coding in @allowed_data_coding,
    do: {:ok, data_coding}

  defp normalize_data_coding(_data_coding), do: :error

  defp stringify_envelope(attributes) do
    attributes
    |> Map.update!(:submit_sm, &stringify_submit_sm/1)
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp stringify_submit_sm(submit_sm),
    do: Map.new(submit_sm, fn {key, value} -> {Atom.to_string(key), value} end)

  defp atomize_known_keys(attributes) do
    Map.new(@fields, fn key -> {key, Map.get(attributes, Atom.to_string(key))} end)
  end
end
