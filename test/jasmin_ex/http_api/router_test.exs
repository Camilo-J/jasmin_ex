defmodule JasminEx.HttpApi.RouterTest do
  use ExUnit.Case, async: true
  import Plug.Conn
  import Plug.Test

  @moduletag :tmp_dir

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Dlr.Config, as: DlrConfig
  alias JasminEx.HttpApi.Metrics
  alias JasminEx.HttpApi.Router
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Router, as: RoutingRouter

  defmodule HttpDlrStore do
    def put(table, key, value, ttl_ms) do
      :ets.insert(table, {key, value, ttl_ms})
      :ok
    end

    def fetch(table, key) do
      case :ets.lookup(table, key) do
        [{^key, value, _ttl_ms}] -> {:ok, value}
        [] -> :missing
      end
    end

    def delete(table, key) do
      case :ets.take(table, key) do
        [{^key, _value, _ttl_ms}] -> :deleted
        [] -> :missing
      end
    end
  end

  defmodule FakeQueue do
    def enqueue(agent, envelope) do
      Agent.get_and_update(agent, fn state ->
        {state.reply, %{state | envelopes: state.envelopes ++ [envelope]}}
      end)
    end

    def start(reply) do
      {:ok, pid} = Agent.start_link(fn -> %{reply: reply, envelopes: []} end)
      pid
    end

    def envelopes(agent), do: Agent.get(agent, & &1.envelopes)
  end

  describe "threat routing" do
    test "GET /send is 405 and does not submit", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      conn = request(env, :get, "/send")

      assert_error(conn, 405, :method_not_allowed)
      assert_no_submit(env)
    end

    test "unknown fields are rejected before submit", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      tags = request(env, :post, "/send", Map.put(send_fields(), "tags", "promo"))
      tlv = request(env, :post, "/send", Map.put(send_fields(), "tlv", "00"))

      assert_error(tags, 400, :unknown_field)
      assert_error(tlv, 400, :unknown_field)
      assert_no_submit(env)
    end

    test "query credentials are rejected", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      conn =
        request(env, :post, "/send?username=alice&password=s3cret", send_fields())

      assert_error(conn, 400, :query_credential)
      assert_no_submit(env)
    end

    test "malformed form and hex are typed validation errors", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      form =
        conn(:post, "/send", "username=alice&password=s3cret&to=%ZZ&from=1616&content=hello")
        |> put_req_header("content-type", "application/x-www-form-urlencoded")
        |> Router.call(env.opts)

      hex =
        request(
          env,
          :post,
          "/send",
          send_fields() |> Map.delete("content") |> Map.put("hex-content", "zz")
        )

      assert_error(form, 400, :malformed_form)
      assert_error(hex, 400, :malformed_hex)
      assert_no_submit(env)
    end
  end

  describe "typed failures" do
    test "auth failure is 401, skips the pipeline, and leaks no secrets", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      bad = request(env, :post, "/send", Map.put(send_fields(), "password", "wrong"))
      missing = request(env, :post, "/send", Map.delete(send_fields(), "password"))

      assert_error(bad, 401, :invalid_credentials)
      assert_error(missing, 401, :invalid_credentials)
      assert_no_submit(env)
    end

    test "rate and balance auth failure is 401 and leaves billing unchanged", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)
      {:ok, _} = Routing.admit(env.router, admission())
      before = billing_view(Routing.snapshot(env.router))

      bad_rate =
        request(env, :post, "/rate", %{
          "username" => "alice",
          "password" => "wrong",
          "to" => "21200000",
          "from" => "1616"
        })

      missing_balance = request(env, :post, "/balance", %{"username" => "alice"})

      assert_error(bad_rate, 401, :invalid_credentials)
      assert_error(missing_balance, 401, :invalid_credentials)
      assert billing_view(Routing.snapshot(env.router)) == before
      assert FakeQueue.envelopes(env.queue) == []
    end

    test "validation errors are 400 and skip later stages", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      missing_to = request(env, :post, "/send", Map.delete(send_fields(), "to"))
      missing_from = request(env, :post, "/send", Map.delete(send_fields(), "from"))
      missing_content = request(env, :post, "/send", Map.delete(send_fields(), "content"))
      empty_content = request(env, :post, "/send", Map.put(send_fields(), "content", ""))

      assert_error(missing_to, 400, :missing_to)
      assert_error(missing_from, 400, :missing_from)
      assert_error(missing_content, 400, :missing_content)
      assert_error(empty_content, 400, :missing_content)
      assert_no_submit(env)
    end

    test "no route is 404 and does not bill or dispatch", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      conn = request(env, :post, "/send", Map.put(send_fields(), "to", "999"))

      assert_error(conn, 404, :no_route)
      assert_no_submit(env)
    end

    test "billing failures are 402 and do not dispatch", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, balance_minor: 50)
      conn = request(env, :post, "/send", send_fields())
      assert_error(conn, 402, :insufficient_balance)
      assert FakeQueue.envelopes(env.queue) == []

      quota = start_http(tmp_dir, file: "routing-q.json", submit_quota: 0)
      quota_conn = request(quota, :post, "/send", send_fields())
      assert_error(quota_conn, 402, :insufficient_quota)
      assert FakeQueue.envelopes(quota.queue) == []
    end

    test "unsupported media type is 415", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      json =
        conn(:post, "/send", "{}")
        |> put_req_header("content-type", "application/json")
        |> Router.call(env.opts)

      missing =
        conn(:post, "/send", URI.encode_query(send_fields()))
        |> Router.call(env.opts)

      assert_error(json, 415, :unsupported_media_type)
      assert_error(missing, 415, :unsupported_media_type)
      assert_no_submit(env)
    end

    test "missing publisher is 503", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, queue: :missing)

      conn = request(env, :post, "/send", send_fields())

      assert_error(conn, 503, :missing_publisher)
      assert Routing.snapshot(env.router).reservations == %{}
    end
  end

  describe "DLR HTTP intake" do
    test "GET /send remains 405 with DLR query fields", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, dlr: :enabled)

      conn = request(env, :get, "/send?dlr=yes&dlr-url=http://example.com/dlr")

      assert_error(conn, 405, :method_not_allowed)
      assert_no_submit(env)
    end

    test "per-request dlr-expiry is unknown 400 with no billing or enqueue", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, dlr: :enabled)

      conn =
        request(env, :post, "/send", Map.put(send_fields(), "dlr-expiry", "3600"))

      assert_error(conn, 400, :unknown_field)
      assert_no_submit(env)
    end

    test "invalid DLR fields are 400 with no billing or enqueue", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, dlr: :enabled)

      level = request(env, :post, "/send", Map.put(send_fields(), "dlr-level", "4"))

      url =
        request(env, :post, "/send", Map.put(send_fields(), "dlr-url", "ftp://example.com/dlr"))

      method = request(env, :post, "/send", Map.put(send_fields(), "dlr-method", "PUT"))
      dlr = request(env, :post, "/send", Map.put(send_fields(), "dlr", "maybe"))

      assert_error(level, 400, :invalid_dlr_level)
      assert_error(url, 400, :invalid_dlr_url)
      assert_error(method, 400, :invalid_dlr_method)
      assert_error(dlr, 400, :invalid_dlr)
      assert_no_submit(env)
    end

    test "dlr_forbidden is HTTP 400 with no billing, map, or enqueue", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, dlr: :enabled)
      {:ok, _} = Routing.set_dlr_level(env.router, "u1", false)
      {:ok, _} = Routing.set_http_set_dlr_method(env.router, "u1", false)

      conn =
        request(
          env,
          :post,
          "/send",
          Map.merge(send_fields(), %{
            "dlr" => "yes",
            "dlr-url" => "http://example.com/dlr"
          })
        )

      assert_error(conn, 400, :dlr_forbidden)
      refute conn.status == 403
      assert_no_submit(env)
    end

    test "DLR-enabling request is 503 when DLR is off", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      conn =
        request(
          env,
          :post,
          "/send",
          Map.merge(send_fields(), %{
            "dlr" => "yes",
            "dlr-url" => "http://example.com/dlr"
          })
        )

      assert_error(conn, 503, :dlr_unavailable)
      assert_no_submit(env)
    end

    test "ordinary send without DLR fields is unchanged when DLR is off", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, id: "mid-plain")

      conn = request(env, :post, "/send", send_fields())

      assert conn.status == 200
      assert conn.resp_body == "mid-plain\n"
      assert [%{gateway_id: "mid-plain"}] = FakeQueue.envelopes(env.queue)
    end

    test "metrics do not introduce HTTP 403 for DLR authorization", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, dlr: :enabled)
      {:ok, _} = Routing.set_dlr_level(env.router, "u1", false)
      {:ok, _} = Routing.set_http_set_dlr_method(env.router, "u1", false)

      _ =
        request(
          env,
          :post,
          "/send",
          Map.merge(send_fields(), %{"dlr" => "yes", "dlr-url" => "http://example.com/dlr"})
        )

      scrape = request(env, :get, "/metrics")
      refute scrape.resp_body =~ ~s(status="403")
      assert scrape.resp_body =~ ~s(jasmin_http_requests_total{endpoint="send",status="400"})
    end
  end

  describe "happy paths" do
    test "POST /send returns a newline-terminated message id", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, id: "mid-http-1")

      conn = request(env, :post, "/send", send_fields())

      assert conn.status == 200
      assert text_plain?(conn)
      assert conn.resp_body == "mid-http-1\n"
      refute_secret(conn)
      assert [%{gateway_id: "mid-http-1"}] = FakeQueue.envelopes(env.queue)
    end

    test "POST /rate and /balance are read-only", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)
      {:ok, _} = Routing.admit(env.router, admission())
      before = billing_view(Routing.snapshot(env.router))

      rate =
        request(env, :post, "/rate", %{
          "username" => "alice",
          "password" => "s3cret",
          "to" => "21200000",
          "from" => "1616"
        })

      balance = request(env, :post, "/balance", %{"username" => "alice", "password" => "s3cret"})

      assert rate.status == 200
      assert rate.resp_body == "100\n"
      assert balance.status == 200
      assert balance.resp_body == "400\n"
      assert billing_view(Routing.snapshot(env.router)) == before
      assert FakeQueue.envelopes(env.queue) == []
    end

    test "GET /ping returns text/plain liveness", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      conn = request(env, :get, "/ping")

      assert conn.status == 200
      assert text_plain?(conn)
      assert conn.resp_body == "pong\n"
    end

    test "POST /send accepts DLR fields when DLR is enabled", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir, dlr: :enabled, id: "mid-dlr-ok")

      conn =
        request(
          env,
          :post,
          "/send",
          Map.merge(send_fields(), %{
            "dlr" => "yes",
            "dlr-url" => "http://example.com/dlr",
            "dlr-level" => "1",
            "dlr-method" => "POST"
          })
        )

      assert conn.status == 200
      assert conn.resp_body == "mid-dlr-ok\n"
      refute_secret(conn)
      assert [%{gateway_id: "mid-dlr-ok"}] = FakeQueue.envelopes(env.queue)
    end

    test "metrics scrape uses only fixed endpoint and status labels", %{tmp_dir: tmp_dir} do
      env = start_http(tmp_dir)

      _ = request(env, :get, "/send")
      _ = request(env, :post, "/send", send_fields())
      conn = request(env, :get, "/metrics")

      assert conn.status == 200
      assert text_plain?(conn)
      assert conn.resp_body =~ ~s(jasmin_http_requests_total{endpoint="send",status="405"})
      assert conn.resp_body =~ ~s(jasmin_http_requests_total{endpoint="send",status="200"})
      refute conn.resp_body =~ "alice"
      refute conn.resp_body =~ "s3cret"
      refute conn.resp_body =~ "21200000"
      refute conn.resp_body =~ "hello"
      refute conn.resp_body =~ "user="

      conn.resp_body
      |> String.split("\n", trim: true)
      |> Enum.filter(&String.starts_with?(&1, "jasmin_http_requests_total"))
      |> Enum.each(fn line ->
        assert line =~ ~r/^jasmin_http_requests_total\{endpoint="[a-z]+",status="\d+"\} \d+$/
      end)
    end
  end

  defp start_http(tmp_dir, opts \\ []) do
    config =
      Config.new(snapshot_path: Path.join(tmp_dir, Keyword.get(opts, :file, "routing.json")))

    router = start_supervised!({RoutingRouter, name: nil, config: config}, id: make_ref())
    {:ok, group} = Routing.put_group(router, gid: "ops")

    {:ok, _user} =
      Routing.put_user(router,
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: Keyword.get(opts, :balance_minor, 500),
        submit_quota: Keyword.get(opts, :submit_quota, 3)
      )

    {:ok, connector} = ConnectorRef.new("smpp-t")
    {:ok, dest} = Filter.Destination.new(address: "21200000")

    {:ok, _route} =
      Routing.put_route(router,
        kind: :static,
        order: 10,
        connector: connector,
        filters: [dest],
        rate_minor: 100,
        precharge_percent: 10
      )

    queue =
      case Keyword.get(opts, :queue, :present) do
        :missing -> nil
        :present -> FakeQueue.start(:ok)
      end

    {:ok, metrics} = Metrics.start_link()
    id = Keyword.get(opts, :id, "mid-http")

    router_opts = %{
      router: router,
      queue: if(queue, do: {FakeQueue, queue}),
      metrics: metrics,
      id_fun: fn -> id end
    }

    router_opts =
      case Keyword.get(opts, :dlr, :off) do
        :enabled ->
          table = :ets.new(:http_dlr_store, [:set, :public])

          Map.merge(router_opts, %{
            dlr_config: DlrConfig.new(enabled: true),
            dlr_store: {HttpDlrStore, table}
          })

        :off ->
          router_opts
      end

    %{router: router, queue: queue, metrics: metrics, opts: router_opts}
  end

  defp request(env, :get, path) do
    conn(:get, path) |> Router.call(env.opts)
  end

  defp request(env, :post, path, fields) do
    conn(:post, path, URI.encode_query(fields))
    |> put_req_header("content-type", "application/x-www-form-urlencoded")
    |> Router.call(env.opts)
  end

  defp send_fields do
    %{
      "username" => "alice",
      "password" => "s3cret",
      "to" => "21200000",
      "from" => "1616",
      "content" => "hello"
    }
  end

  defp assert_error(conn, status, reason) do
    assert conn.status == status
    assert text_plain?(conn)
    assert conn.resp_body == "error:#{reason}\n"
    refute_secret(conn)
  end

  defp assert_no_submit(env) do
    if env.queue, do: assert(FakeQueue.envelopes(env.queue) == [])
    assert Routing.snapshot(env.router).reservations == %{}
  end

  defp text_plain?(conn) do
    Enum.any?(get_resp_header(conn, "content-type"), &String.starts_with?(&1, "text/plain"))
  end

  defp refute_secret(conn) do
    refute conn.resp_body =~ "s3cret"
    inspected = inspect(conn.resp_body)
    refute inspected =~ "s3cret"
  end

  defp admission do
    {:ok, bill} =
      Bill.new(
        bill_id: "bill-1",
        uid: "u1",
        route_order: 10,
        rate_minor: 100,
        precharge_percent: 10
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1_000)
    admission
  end

  defp billing_view(state) do
    %{
      revision: state.revision,
      reservations: state.reservations,
      balances: Map.new(state.users, fn {uid, user} -> {uid, user.balance_minor} end),
      quotas: Map.new(state.users, fn {uid, user} -> {uid, user.submit_quota} end)
    }
  end
end
