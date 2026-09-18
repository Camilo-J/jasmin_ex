defmodule JasminEx.Dlr.HttpThrower do
  @moduledoc false

  alias JasminEx.Dlr.HttpJob

  @ack "ACK/Jasmin"

  @spec process(binary(), map(), keyword()) :: :ok | :retry | :terminal
  def process(payload, _meta, context) when is_binary(payload) and is_list(context) do
    with {:ok, job} <- HttpJob.decode(payload),
         :ok <- fresh(job, context),
         {:ok, request} <- request(job) do
      context
      |> Keyword.fetch!(:client)
      |> invoke(request)
      |> classify()
    else
      _other -> :terminal
    end
  end

  defp request(%{method: "GET"} = job) do
    uri = URI.parse(job.url)
    query = merge_query(uri.query, job.fields)
    {:ok, %{method: "GET", url: URI.to_string(%{uri | query: query}), headers: [], body: ""}}
  rescue
    _error -> {:error, :invalid_url}
  end

  defp request(%{method: "POST"} = job) do
    {:ok,
     %{
       method: "POST",
       url: job.url,
       headers: [{"content-type", "application/x-www-form-urlencoded"}],
       body: URI.encode_query(job.fields)
     }}
  end

  defp merge_query(nil, fields), do: URI.encode_query(fields)

  defp merge_query(existing, fields) do
    existing
    |> URI.decode_query()
    |> Map.merge(fields)
    |> URI.encode_query()
  end

  defp invoke({module, client_context}, request), do: module.request(client_context, request)
  defp invoke(client, request) when is_function(client, 1), do: client.(request)

  defp classify({:ok, 404, _body}), do: :terminal

  defp classify({:ok, status, body})
       when is_integer(status) and status < 400 and is_binary(body) do
    if String.trim(body) == @ack, do: :ok, else: :retry
  end

  defp classify({:ok, _status, _body}), do: :retry
  defp classify({:error, _reason}), do: :retry
  defp classify(_other), do: :retry

  defp fresh(job, context) do
    {module, clock_context} = Keyword.fetch!(context, :clock)
    if module.now_ms(clock_context) < job.deadline_ms, do: :ok, else: {:error, :expired}
  end
end
