defmodule JasminEx.HttpApi.SubmitEncodingPathTest do
  @moduledoc """
  Local truth for HTTP encoding: Router.call -> production pipeline ->
  captured FakeQueue -> envelope v2 round-trip -> Client.send_submit_sm ->
  FakeSMSC raw submit_sm bytes. This is not broker or live SMSC evidence.
  """

  use ExUnit.Case, async: false
  import Plug.Conn
  import Plug.Test

  @moduletag :tmp_dir

  alias JasminEx.HttpApi.Metrics
  alias JasminEx.HttpApi.Router
  alias JasminEx.Messaging.Envelope
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Router, as: RoutingRouter
  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body

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

  @wire_cases [
    {"gsm-hello", %{"content" => "hello", "coding" => "0"}, "hello", :SMSC_DEFAULT_ALPHABET},
    {"gsm-euro", %{"content" => "€", "coding" => "0"}, <<0x1B, 0x65>>, :SMSC_DEFAULT_ALPHABET},
    {"ascii", %{"content" => "Hi", "coding" => "1"}, "Hi", :IA5_ASCII},
    {"octet-nul", %{"hex-content" => "00FF", "coding" => "2"}, <<0x00, 0xFF>>,
     :OCTET_UNSPECIFIED},
    {"latin1", %{"content" => "é", "coding" => "3"}, <<0xE9>>, :LATIN_1},
    {"ucs2", %{"content" => "Hi", "coding" => "8"}, <<0x00, 0x48, 0x00, 0x69>>, :UCS2},
    {"ucs2-hex", %{"hex-content" => "00480069", "coding" => "8"}, <<0x00, 0x48, 0x00, 0x69>>,
     :UCS2},
    {"ucs2-emoji", %{"content" => "🚀", "coding" => "8"}, <<0xD8, 0x3D, 0xDE, 0x80>>, :UCS2}
  ]

  test "Router production path reaches FakeSMSC with exact wire octets and sm_length", %{
    tmp_dir: tmp_dir
  } do
    for {name, fields, wire, data_coding} <- @wire_cases do
      env = start_http(tmp_dir, file: "routing-#{name}.json", id: "mid-#{name}")
      conn = request(env, send_fields(fields))
      assert conn.status == 200, "#{name}: #{conn.resp_body}"

      [envelope] = FakeQueue.envelopes(env.queue)
      {:ok, encoded} = Envelope.encode(envelope)
      {:ok, decoded} = Envelope.decode(encoded)
      assert decoded.submit_sm.short_message == wire
      assert decoded.submit_sm.short_message == envelope.submit_sm.short_message
      assert decoded.submit_sm.data_coding == String.to_integer(fields["coding"])

      {sm_length, octets, pdu_coding} = submit_on_fake_smsc(decoded)
      assert sm_length == byte_size(wire)
      assert octets == wire
      assert pdu_coding == data_coding
    end
  end

  test "254 encoded octets are on the wire and 255 never bills or enqueues", %{tmp_dir: tmp_dir} do
    env = start_http(tmp_dir, id: "mid-254")
    conn = request(env, send_fields(%{"content" => String.duplicate("a", 254)}))
    assert conn.status == 200
    [envelope] = FakeQueue.envelopes(env.queue)
    {:ok, encoded} = Envelope.encode(envelope)
    {:ok, decoded} = Envelope.decode(encoded)
    {sm_length, octets, pdu_coding} = submit_on_fake_smsc(decoded)
    assert sm_length == 254
    assert byte_size(octets) == 254
    assert pdu_coding == :SMSC_DEFAULT_ALPHABET

    over = start_http(tmp_dir, file: "routing-255.json")
    too_long = request(over, send_fields(%{"content" => String.duplicate("a", 255)}))
    assert too_long.status == 400
    assert too_long.resp_body == "error:message_too_long\n"
    assert FakeQueue.envelopes(over.queue) == []
    assert Routing.snapshot(over.router).reservations == %{}
  end

  test "invalid text, hex syntax, and hex structure never bill, DLR, or enqueue", %{
    tmp_dir: tmp_dir
  } do
    env = start_http(tmp_dir, dlr: :enabled)
    before = billing_view(Routing.snapshot(env.router))
    dlr_fields = %{"dlr" => "yes", "dlr-url" => "http://example.com/dlr"}

    ascii = request(env, send_fields(Map.merge(%{"content" => "é", "coding" => "1"}, dlr_fields)))

    hex =
      request(env, send_fields(Map.merge(%{"hex-content" => "zz", "coding" => "2"}, dlr_fields)))

    structure =
      request(env, send_fields(Map.merge(%{"hex-content" => "00", "coding" => "8"}, dlr_fields)))

    assert ascii.status == 400 and ascii.resp_body == "error:invalid_content\n"
    assert hex.status == 400 and hex.resp_body == "error:malformed_hex\n"
    assert structure.status == 400 and structure.resp_body == "error:invalid_content\n"
    assert FakeQueue.envelopes(env.queue) == []
    assert billing_view(Routing.snapshot(env.router)) == before
    assert Routing.snapshot(env.router).reservations == %{}
    assert dlr_store_empty?(env)
  end

  defp submit_on_fake_smsc(envelope) do
    {:ok, port, smsc} = FakeSMSC.start_link()
    ref = FakeSMSC.subscribe(smsc)

    {:ok, client} =
      Client.start_link(
        connector_id: envelope.connector_id,
        host: ~c"localhost",
        port: port,
        system_id: "user",
        password: "pw",
        system_type: "type",
        bind_as: :transmitter,
        heartbeat_ms: 10_000,
        response_timeout_ms: 200,
        reconnect_base_ms: 5,
        reconnect_cap_ms: 5,
        reconnect_jitter: false
      )

    try do
      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      assert {:ok, "fake-msg-id"} =
               Client.send_submit_sm(client, struct(Body.SubmitSM, envelope.submit_sm))

      raw_submit_sm(await_submit_bytes(ref))
    after
      stop_pid(client)
      stop_pid(smsc)
    end
  end

  defp raw_submit_sm(payload) do
    {:ok, %PDU{command: :submit_sm, body: body}} = PDU.decode(payload)
    {:ok, decoded} = Body.decode(:submit_sm, body)
    sm = decoded.short_message
    sm_size = byte_size(sm)
    prefix_size = byte_size(body) - sm_size - 1
    <<_prefix::binary-size(^prefix_size), sm_length, octets::binary-size(^sm_size)>> = body
    {sm_length, octets, decoded.data_coding}
  end

  defp await_submit_bytes(ref) do
    receive do
      {:fake_smsc_bytes, ^ref, payload} ->
        case PDU.decode(payload) do
          {:ok, %PDU{command: :submit_sm}} -> payload
          _other -> await_submit_bytes(ref)
        end
    after
      1_000 -> flunk("did not receive submit_sm bytes on FakeSMSC")
    end
  end

  defp wait_until(predicate, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(predicate, deadline)
  end

  defp do_wait(predicate, deadline) do
    if predicate.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        {:error, :timeout}
      else
        Process.sleep(5)
        do_wait(predicate, deadline)
      end
    end
  end

  defp stop_pid(pid) do
    if is_pid(pid) and Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 200)
      catch
        _, _ -> :ok
      end
    end
  end

  defp start_http(tmp_dir, opts) do
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
        balance_minor: 500,
        submit_quota: 8
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

    queue = FakeQueue.start(:ok)
    {:ok, metrics} = Metrics.start_link()

    router_opts = %{
      router: router,
      queue: {FakeQueue, queue},
      metrics: metrics,
      id_fun: fn -> Keyword.get(opts, :id, "mid-path") end
    }

    router_opts =
      case Keyword.get(opts, :dlr, :off) do
        :enabled ->
          table = :ets.new(:http_path_dlr, [:set, :public])

          Map.merge(router_opts, %{
            dlr_config: JasminEx.Dlr.Config.new(enabled: true),
            dlr_store: {__MODULE__.HttpDlrStore, table}
          })

        :off ->
          router_opts
      end

    %{router: router, queue: queue, opts: router_opts}
  end

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

  defp request(env, fields) do
    conn(:post, "/send", URI.encode_query(fields))
    |> put_req_header("content-type", "application/x-www-form-urlencoded")
    |> Router.call(env.opts)
  end

  defp send_fields(fields) do
    %{
      "username" => "alice",
      "password" => "s3cret",
      "to" => "21200000",
      "from" => "1616"
    }
    |> Map.merge(fields)
  end

  defp dlr_store_empty?(env) do
    case env.opts do
      %{dlr_store: {HttpDlrStore, table}} -> :ets.info(table, :size) == 0
      _missing -> false
    end
  end

  defp billing_view(state) do
    %{
      reservations: state.reservations,
      balances: Map.new(state.users, fn {uid, user} -> {uid, user.balance_minor} end),
      quotas: Map.new(state.users, fn {uid, user} -> {uid, user.submit_quota} end)
    }
  end
end
