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

    unknown = ~s({"version":2,"kind":"http_job","#{unknown_key}":"x"})
    assert {:error, :invalid_job} = HttpJob.decode(unknown)
    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end

    assert {:error, :unsupported_version} =
             HttpJob.decode(~s({"version":1,"kind":"http_job"}))
  end

  test "the job payload must be a unified contract payload for its own message and level" do
    for payload <- [
          %{job().payload | "level" => 2},
          %{job().payload | "id" => "other"},
          %{job().payload | "schema_version" => 2},
          %{job().payload | "status_origin" => "smsc"},
          %{job().payload | "status" => "DELIVRD"},
          %{job().payload | "segments" => []},
          Map.delete(job().payload, "total_segments"),
          %{
            job().payload
            | "segments" => [%{"index" => 1, "text" => String.duplicate("x", 70_000)}]
          }
        ] do
      assert {:error, :invalid_job} = HttpJob.encode(%{job() | payload: payload})
    end
  end

  test "POST sends the payload as application/json" do
    {context, agent} = context([{:ok, 204, "ACK/Jasmin"}])

    assert :ok = HttpThrower.process(encoded_job(), %{}, context)
    [request] = requests(agent)
    assert request.method == "POST"
    assert request.url == "https://example.com/callback"
    assert {"content-type", "application/json"} in request.headers
    assert :json.decode(request.body) == job().payload
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
      level: 1,
      created_at_ms: 1_000,
      deadline_ms: 80_000,
      payload: %{
        "schema_version" => 1,
        "id" => "G1",
        "level" => 1,
        "status" => "ACCEPTED",
        "status_origin" => "gateway",
        "connector" => "c1",
        "total_segments" => 1,
        "segments" => [%{"index" => 1, "id_smsc" => "abc", "message_status" => "ESME_ROK"}]
      }
    }
  end
end
