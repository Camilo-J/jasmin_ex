defmodule JasminEx.Messaging.Envelope do
  @moduledoc """
  Represents and serializes a queued messaging request.

  Encode writes integer version 2. The wire `submit_sm` field is
  `short_message_base64` (standard padded canonical Base64). The in-memory
  struct still holds binary `submit_sm.short_message`. Version 1 JSON is
  decoded as original text bytes and is never Base64-decoded or transcoded.
  """

  @v1 1
  @v2 2
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
      |> Map.put(:version, @v2)
      |> stringify_envelope()
      |> :json.encode()
      |> IO.iodata_to_binary()

    {:ok, payload}
  end

  def decode(payload) when is_binary(payload) do
    with {:ok, attributes} <- decode_json(payload),
         :ok <- validate_version(attributes),
         {:ok, attributes} <- materialize_submit_sm(attributes) do
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

  defp validate_version(%{"version" => version}) when version in [@v1, @v2], do: :ok
  defp validate_version(%{"version" => _version}), do: {:error, :unsupported_version}
  defp validate_version(_attributes), do: {:error, :invalid_envelope}

  defp materialize_submit_sm(%{"version" => @v2, "submit_sm" => submit} = attributes)
       when is_map(submit) do
    if Map.has_key?(submit, "short_message") do
      {:error, :invalid_envelope}
    else
      case decode_v2_short_message(submit) do
        {:ok, submit} -> {:ok, Map.put(attributes, "submit_sm", submit)}
        error -> error
      end
    end
  end

  defp materialize_submit_sm(%{"version" => @v2}), do: {:error, :invalid_envelope}
  defp materialize_submit_sm(attributes), do: {:ok, attributes}

  defp decode_v2_short_message(submit) do
    case Map.fetch(submit, "short_message_base64") do
      {:ok, encoded} when is_binary(encoded) ->
        case canonical_base64(encoded) do
          {:ok, bytes} ->
            {:ok,
             submit
             |> Map.delete("short_message_base64")
             |> Map.put("short_message", bytes)
             |> absents_v2_json_null()}

          :error ->
            {:error, :invalid_envelope}
        end

      _ ->
        {:error, :invalid_envelope}
    end
  end

  defp canonical_base64(encoded) do
    case Base.decode64(encoded) do
      {:ok, bytes} ->
        if Base.encode64(bytes) == encoded, do: {:ok, bytes}, else: :error

      :error ->
        :error
    end
  end

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
           data_coding: Map.get(submit_sm, "data_coding"),
           registered_delivery: Map.get(submit_sm, "registered_delivery")
         })

  defp validate_submit_sm(
         %{
           source_addr: source,
           destination_addr: destination,
           short_message: message
         } = submit_sm
       )
       when is_binary(source) and is_binary(destination) and is_binary(message) do
    with {:ok, data_coding} <- normalize_data_coding(Map.get(submit_sm, :data_coding)),
         {:ok, registered_delivery} <-
           normalize_registered_delivery(Map.get(submit_sm, :registered_delivery)) do
      {:ok,
       %{
         source_addr: source,
         destination_addr: destination,
         short_message: message,
         data_coding: data_coding,
         registered_delivery: registered_delivery
       }}
    else
      :error -> {:error, :invalid_submit_sm}
    end
  end

  defp validate_submit_sm(_submit_sm), do: {:error, :invalid_submit_sm}

  defp absents_v2_json_null(submit) do
    submit
    |> replace_v2_json_null("data_coding")
    |> replace_v2_json_null("registered_delivery")
  end

  defp replace_v2_json_null(submit, key) do
    case Map.fetch(submit, key) do
      {:ok, :null} -> Map.put(submit, key, nil)
      _ -> submit
    end
  end

  defp normalize_data_coding(nil), do: {:ok, 0}

  defp normalize_data_coding(data_coding) when data_coding in @allowed_data_coding,
    do: {:ok, data_coding}

  defp normalize_data_coding(_data_coding), do: :error

  defp normalize_registered_delivery(nil), do: {:ok, 0}
  defp normalize_registered_delivery(0), do: {:ok, 0}
  defp normalize_registered_delivery(1), do: {:ok, 1}
  defp normalize_registered_delivery(_value), do: :error

  defp stringify_envelope(attributes) do
    attributes
    |> Map.update!(:submit_sm, &stringify_submit_sm/1)
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp stringify_submit_sm(submit_sm) do
    message = Map.fetch!(submit_sm, :short_message)

    submit_sm
    |> Map.delete(:short_message)
    |> Map.put(:short_message_base64, Base.encode64(message))
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp atomize_known_keys(attributes) do
    Map.new(@fields, fn key -> {key, Map.get(attributes, Atom.to_string(key))} end)
  end
end
