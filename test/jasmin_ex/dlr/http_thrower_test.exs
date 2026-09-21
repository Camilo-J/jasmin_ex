defmodule JasminEx.Dlr.HttpThrowerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.{HttpJob, HttpThrower}

  defmodule Clock do
    def now_ms(value), do: value
  end

  defmodule Client do
    def request(agent, request) do
      Agent.get_and_update(agent, fn %{responses: [response | rest]} = state ->
        {response, %{state | requests: [request | state.requests], responses: rest}}
      end)
    end
  end

  test "HTTP job codec is versioned, bounded, and atom-safe" do
    assert {:ok, encoded} = HttpJob.encode(job())
    assert {:ok, decoded} = HttpJob.decode(encoded)
    assert decoded == job()

    unknown_key = "unexpected_atom_#{System.unique_integer([:positive])}"
    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end

    unknown = ~s({"version":1,"kind":"http_job","#{unknown_key}":"x"})
    assert {:error, :invalid_job} = HttpJob.decode(unknown)
    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end

    assert {:error, :unsupported_version} =
             HttpJob.decode(~s({"version":2,"kind":"http_job"}))

    assert {:error, :invalid_job} = HttpJob.encode(%{job() | method: "PUT"})
  end

  test "GET appends exact callback fields as query parameters" do
    {context, agent} = context([{:ok, 200, "  ACK/Jasmin\n"}])
    payload = encoded_job(%{method: "GET", url: "https://example.com/callback?tenant=one"})

    assert :ok = HttpThrower.process(payload, %{}, context)
    [request] = requests(agent)
    uri = URI.parse(request.url)
    assert request.method == "GET"
    assert request.body == ""
    assert URI.decode_query(uri.query)["tenant"] == "one"
    assert URI.decode_query(uri.query)["id"] == "G1"
    assert URI.decode_query(uri.query)["connector"] == "c1"
  end

  test "POST sends application/x-www-form-urlencoded fields" do
    {context, agent} = context([{:ok, 204, "ACK/Jasmin"}])

    assert :ok = HttpThrower.process(encoded_job(), %{}, context)
    [request] = requests(agent)
    assert request.method == "POST"
    assert {"content-type", "application/x-www-form-urlencoded"} in request.headers
    assert URI.decode_query(request.body) == job().fields
  end

  test "status below 400 plus exact stripped ACK succeeds without following redirect" do
    for status <- [200, 302, 399] do
      {context, agent} = context([{:ok, status, "\tACK/Jasmin \r\n"}])
      assert :ok = HttpThrower.process(encoded_job(), %{}, context)
      assert length(requests(agent)) == 1
    end
  end

  test "404 is terminal while wrong ACK, other statuses, and transport failures retry" do
    for {response, expected} <- [
          {{:ok, 404, "ACK/Jasmin"}, :terminal},
          {{:ok, 200, "ack/jasmin"}, :retry},
          {{:ok, 200, "prefix ACK/Jasmin"}, :retry},
          {{:ok, 400, "ACK/Jasmin"}, :retry},
          {{:ok, 500, "ACK/Jasmin"}, :retry},
          {{:error, :timeout}, :retry}
        ] do
      {context, agent} = context([response])
      assert expected == HttpThrower.process(encoded_job(), %{}, context)
      assert length(requests(agent)) == 1
    end
  end

  test "expired or malformed jobs are terminal without a network attempt" do
    {context, agent} = context([{:ok, 200, "ACK/Jasmin"}], 80_000)
    assert :terminal = HttpThrower.process(encoded_job(), %{}, context)
    assert [] = requests(agent)

    assert :terminal = HttpThrower.process("not-json", %{}, context)
    assert [] = requests(agent)
  end

  test "one processing call invokes the client exactly once" do
    {context, agent} = context([{:error, :timeout}, {:ok, 200, "ACK/Jasmin"}])
    assert :retry = HttpThrower.process(encoded_job(), %{}, context)
    assert length(requests(agent)) == 1
  end

  defp context(responses, now_ms \\ 1_000) do
    {:ok, agent} = Agent.start_link(fn -> %{requests: [], responses: responses} end)
    {[client: {Client, agent}, clock: {Clock, now_ms}], agent}
  end

  defp requests(agent), do: Agent.get(agent, &Enum.reverse(&1.requests))

  defp encoded_job(overrides \\ %{}) do
    assert {:ok, payload} = HttpJob.encode(Map.merge(job(), overrides))
    payload
  end

  defp job do
    %{
      job_id: "job-1",
      event_id: "event-1",
      gateway_id: "G1",
      url: "https://example.com/callback",
      method: "POST",
      level: 1,
      created_at_ms: 1_000,
      deadline_ms: 80_000,
      fields: %{
        "id" => "G1",
        "level" => "1",
        "message_status" => "ESME_ROK",
        "connector" => "c1"
      }
    }
  end
end
