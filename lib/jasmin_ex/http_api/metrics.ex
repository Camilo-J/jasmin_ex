defmodule JasminEx.HttpApi.Metrics do
  @moduledoc false

  @endpoints [:send, :rate, :balance, :ping, :metrics]
  @statuses [200, 400, 401, 402, 404, 405, 415, 500, 503]

  def start_link(opts \\ []) when is_list(opts) do
    Agent.start_link(fn -> %{} end, Keyword.take(opts, [:name]))
  end

  def record(nil, _endpoint, _status), do: :ok

  def record(server, endpoint, status)
      when endpoint in @endpoints and status in @statuses do
    Agent.update(server, fn counters ->
      Map.update(counters, {endpoint, status}, 1, &(&1 + 1))
    end)
  end

  def record(_server, _endpoint, _status), do: :ok

  def scrape(nil), do: "# TYPE jasmin_http_requests_total counter\n"

  def scrape(server) do
    counters = Agent.get(server, & &1)

    lines =
      counters
      |> Enum.sort()
      |> Enum.map(fn {{endpoint, status}, count} ->
        ~s(jasmin_http_requests_total{endpoint="#{endpoint}",status="#{status}"} #{count}\n)
      end)

    ["# TYPE jasmin_http_requests_total counter\n" | lines]
    |> IO.iodata_to_binary()
  end
end
