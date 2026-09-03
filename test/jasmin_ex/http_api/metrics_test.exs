defmodule JasminEx.HttpApi.MetricsTest do
  use ExUnit.Case, async: true

  alias JasminEx.HttpApi.Metrics

  test "scrape uses only fixed endpoint and status labels" do
    {:ok, metrics} = Metrics.start_link()
    assert :ok = Metrics.record(metrics, :send, 200)
    assert :ok = Metrics.record(metrics, :send, 401)

    scrape = Metrics.scrape(metrics)

    assert scrape =~ ~s(jasmin_http_requests_total{endpoint="send",status="200"})
    assert scrape =~ ~s(jasmin_http_requests_total{endpoint="send",status="401"})
    refute scrape =~ "username"
    refute scrape =~ "uid"
    refute scrape =~ "content"
    refute scrape =~ "alice"
    refute scrape =~ "user="

    assert_fixed_labels(scrape)
  end

  test "a different endpoint stays on the same fixed dimensions" do
    {:ok, metrics} = Metrics.start_link()
    assert :ok = Metrics.record(metrics, :ping, 200)
    assert :ok = Metrics.record(metrics, :rate, 402)

    scrape = Metrics.scrape(metrics)

    assert scrape =~ ~s(jasmin_http_requests_total{endpoint="ping",status="200"})
    assert scrape =~ ~s(jasmin_http_requests_total{endpoint="rate",status="402"})
    refute scrape =~ "to="
    refute scrape =~ "password"
    assert_fixed_labels(scrape)
  end

  defp assert_fixed_labels(scrape) do
    scrape
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.starts_with?(&1, "jasmin_http_requests_total"))
    |> Enum.each(fn line ->
      assert line =~ ~r/^jasmin_http_requests_total\{endpoint="[a-z]+",status="\d+"\} \d+$/
    end)
  end
end
