defmodule JasminEx.Messaging.Envelope do
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

  def new(attributes) when is_map(attributes) do
    with {:ok, submit_sm} <- validate_submit_sm(Map.get(attributes, :submit_sm)),
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

  defp validate_submit_sm(%{
         "source_addr" => source,
         "destination_addr" => destination,
         "short_message" => message
       }),
       do:
         validate_submit_sm(%{
           source_addr: source,
           destination_addr: destination,
           short_message: message
         })

  defp validate_submit_sm(%{
         source_addr: source,
         destination_addr: destination,
         short_message: message
       })
       when is_binary(source) and is_binary(destination) and is_binary(message),
       do: {:ok, %{source_addr: source, destination_addr: destination, short_message: message}}

  defp validate_submit_sm(_submit_sm), do: {:error, :invalid_submit_sm}

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
