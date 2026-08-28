defmodule JasminEx.Smpp.Server.SessionTest do
  use ExUnit.Case, async: false
  @moduletag :tmp_dir

  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.FakeESME
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.Server.{BindingManager, Session}

  @tcp [:binary, packet: :raw, active: false, reuseaddr: true, ip: {127, 0, 0, 1}]

  test "frames close unregistered; bind/enquire/unbind work; submit_sm fails", %{tmp_dir: tmp_dir} do
    {router, mgr} = seed(tmp_dir)

    for payload <- [
          <<0::32, 0::32, 0::32, 0::32>>,
          <<70_000::32, 0::32, 0::32, 0::32>>,
          <<12::32, 0::32, 0::32, 0::32>>
        ] do
      {session, esme} = open(router, mgr)
      ref = Process.monitor(session)
      :ok = FakeESME.send_raw(esme, payload)
      assert_receive {:DOWN, ^ref, :process, ^session, _}
      assert BindingManager.count(mgr, "alice") == 0
      FakeESME.close(esme)
    end

    {session, esme} = open(router, mgr)
    {:ok, bind} = FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret", 9)

    assert {bind.command, bind.status, bind.sequence_number} ==
             {:bind_transmitter_resp, :ESME_ROK, 9}

    {:ok, enq} = FakeESME.enquire_link(esme, 10)
    assert {enq.command, enq.status, enq.sequence_number} == {:enquire_link_resp, :ESME_ROK, 10}
    {:ok, sub} = FakeESME.submit_sm(esme, 11)
    assert {sub.command, sub.status} == {:submit_sm_resp, :ESME_RSUBMITFAIL}
    refute Code.ensure_loaded?(JasminEx.MtSubmitPipeline)
    {:ok, unb} = FakeESME.unbind(esme, 12)
    assert {unb.command, unb.status} == {:unbind_resp, :ESME_ROK}
    ref = Process.monitor(session)
    assert_receive {:DOWN, ^ref, :process, ^session, _}
    assert BindingManager.count(mgr, "alice") == 0

    {_s1, tx} = open(router, mgr)
    {_s2, rx} = open(router, mgr)
    {_s3, trx} = open(router, mgr)
    {:ok, a} = FakeESME.bind(tx, :bind_transmitter, "alice", "smpp-secret")
    {:ok, b} = FakeESME.bind(rx, :bind_receiver, "alice", "smpp-secret")
    {:ok, c} = FakeESME.bind(trx, :bind_transceiver, "alice", "smpp-secret")
    assert [a.status, b.status, c.status] == [:ESME_ROK, :ESME_ROK, :ESME_ROK]
    assert BindingManager.count(mgr, "alice") == 3
    {:ok, fail} = FakeESME.bind(elem(open(router, mgr), 1), :bind_transmitter, "alice", "wrong")
    assert fail.status == :ESME_RBINDFAIL
    refute inspect(fail) =~ "wrong"
  end

  test "rejected inbound submit_sm does not affect a live southbound connector", %{
    tmp_dir: tmp_dir
  } do
    {router, mgr} = seed(tmp_dir)
    {:ok, port, smsc} = FakeSMSC.start_link()
    on_exit(fn -> if Process.alive?(smsc), do: GenServer.stop(smsc) end)

    supervisor =
      start_supervised!(
        {ConnectorSupervisor,
         [
           connector_id: "connector-#{port}",
           host: ~c"localhost",
           port: port,
           system_id: "user",
           password: "pw",
           system_type: "type",
           bind_as: :transmitter,
           heartbeat_ms: 10_000,
           response_timeout_ms: 100,
           reconnect_base_ms: 5,
           reconnect_cap_ms: 5,
           reconnect_jitter: false
         ]}
      )

    connector = child_pid(supervisor, {:smpp_connector, "connector-#{port}"})
    client = child_pid(connector, :smpp_client)
    assert wait_bound(client) == :ok
    pdu_ref = FakeSMSC.subscribe_pdus(smsc)
    {before_state, before_data} = :sys.get_state(client)
    before_window = before_data.request_window

    {_session, esme} = open(router, mgr)
    {:ok, %{status: :ESME_ROK}} = FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret")
    {:ok, sub} = FakeESME.submit_sm(esme, 11)
    assert {sub.command, sub.status} == {:submit_sm_resp, :ESME_RSUBMITFAIL}

    refute_receive {:fake_smsc_pdu, ^pdu_ref, %PDU{command: :submit_sm}}, 50
    refute_receive {:fake_smsc_pdu, ^pdu_ref, _}, 50
    assert Client.status(client) == :bound
    assert child_pid(connector, :smpp_client) == client
    {after_state, after_data} = :sys.get_state(client)

    assert {after_state, after_data.socket, after_data.request_window} ==
             {before_state, before_data.socket, before_window}

    assert after_data.request_window.pending == %{}
    FakeESME.close(esme)
  end

  test "concurrent limit-1 admits one bind; disconnect and crash release once", %{
    tmp_dir: tmp_dir
  } do
    {router, mgr} = seed(tmp_dir, 1)
    {s1, e1} = open(router, mgr)
    {s2, e2} = open(router, mgr)
    parent = self()
    go = make_ref()

    tasks =
      Enum.map([e1, e2], fn esme ->
        Task.async(fn ->
          send(parent, :ready)
          receive do: (^go -> bind_tx(esme))
        end)
      end)

    assert_receive :ready
    assert_receive :ready
    Enum.each(tasks, &send(&1.pid, go))

    results =
      tasks
      |> Task.await_many()
      |> Enum.map(fn {:ok, %{status: status}} -> status end)

    assert Enum.sort(results) == [:ESME_RBINDFAIL, :ESME_ROK]
    assert BindingManager.count(mgr, "alice") == 1

    for {session, esme} <- [{s1, e1}, {s2, e2}] do
      ref = Process.monitor(session)
      FakeESME.close(esme)
      assert_receive {:DOWN, ^ref, :process, ^session, _}
    end

    assert BindingManager.count(mgr, "alice") == 0

    {session, esme} = open(router, mgr)
    {:ok, %{status: :ESME_ROK}} = FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret")
    ref = Process.monitor(session)
    FakeESME.close(esme)
    assert_receive {:DOWN, ^ref, :process, ^session, _}
    assert BindingManager.count(mgr, "alice") == 0

    {session, esme} = open(router, mgr)
    {:ok, %{status: :ESME_ROK}} = FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret")
    ref = Process.monitor(session)
    Process.unlink(session)
    Process.exit(session, :kill)
    assert_receive {:DOWN, ^ref, :process, ^session, _}
    assert BindingManager.count(mgr, "alice") == 0
    FakeESME.close(esme)
  end

  defp seed(tmp_dir, limit \\ 3) do
    router =
      start_supervised!(
        {Routing.Router,
         name: nil, config: Config.new(snapshot_path: Path.join(tmp_dir, "r.json"))}
      )

    {:ok, g} = Routing.put_group(router, gid: "ops")
    {:ok, _} = Routing.put_user(router, uid: "u1", username: "alice", secret: "pw", group: g)
    {:ok, _} = Routing.set_smpp_secret(router, "u1", "smpp-secret")
    {:ok, _} = Routing.set_max_bindings(router, "u1", limit)
    {router, start_supervised!(BindingManager)}
  end

  defp bind_tx(esme), do: FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret")

  defp open(router, mgr) do
    {:ok, ls} = :gen_tcp.listen(0, @tcp)
    {:ok, port} = :inet.port(ls)
    {:ok, session} = Session.start_link(router: router, binding_manager: mgr)
    parent = self()

    spawn(fn ->
      {:ok, sock} = :gen_tcp.accept(ls)
      :ok = :gen_tcp.controlling_process(sock, session)
      :ok = Session.handoff(session, sock)
      send(parent, :handed)
    end)

    {:ok, esme} = FakeESME.connect(port)
    assert_receive :handed, 1_000
    {session, esme}
  end

  defp child_pid(supervisor, id) do
    {^id, pid, _type, _modules} =
      Enum.find(Supervisor.which_children(supervisor), &(elem(&1, 0) == id))

    pid
  end

  defp wait_bound(client, remaining \\ 200) do
    cond do
      Client.status(client) == :bound ->
        :ok

      remaining <= 0 ->
        {:error, :timeout}

      true ->
        Process.sleep(5)
        wait_bound(client, remaining - 1)
    end
  end
end
