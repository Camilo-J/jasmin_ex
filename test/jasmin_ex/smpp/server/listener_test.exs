defmodule JasminEx.Smpp.Server.ListenerTest do
  use ExUnit.Case, async: false
  @moduletag :tmp_dir

  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Smpp.FakeESME
  alias JasminEx.Smpp.Server
  alias JasminEx.Smpp.Server.{BindingManager, Listener, SessionSupervisor}
  alias JasminEx.Smpp.Server.Supervisor, as: ServerSupervisor

  test "config defaults to disabled loopback 2775" do
    cfg = Server.Config.new([])
    assert {cfg.enabled, cfg.port, cfg.host} == {false, 2775, {127, 0, 0, 1}}

    enabled = Server.Config.new(enabled: true, port: 0)
    assert {enabled.enabled, enabled.port} == {true, 0}
  end

  test "listener hands off accepted sockets to temporary sessions", %{tmp_dir: tmp_dir} do
    {router, sup, mgr} = start_server(tmp_dir)
    port = listener_port(sup)
    {:ok, esme} = FakeESME.connect(port)
    {:ok, bind} = FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret", 9)

    assert {bind.command, bind.status, bind.sequence_number} ==
             {:bind_transmitter_resp, :ESME_ROK, 9}

    assert BindingManager.count(mgr, "alice") == 1
    assert session_count(sup) == 1

    [{_, session, :worker, _}] = DynamicSupervisor.which_children(sessions(sup))
    ref = Process.monitor(session)
    Process.exit(session, :kill)
    assert_receive {:DOWN, ^ref, :process, ^session, _}
    assert session_count(sup) == 0
    assert BindingManager.count(mgr, "alice") == 0
    FakeESME.close(esme)

    {:ok, esme} = FakeESME.connect(port)
    {:ok, again} = FakeESME.bind(esme, :bind_transmitter, "alice", "smpp-secret")
    assert again.status == :ESME_ROK
    {:ok, unb} = FakeESME.unbind(esme)
    assert unb.status == :ESME_ROK
    Process.sleep(50)
    assert session_count(sup) == 0
    FakeESME.close(esme)
    Supervisor.stop(sup)
    GenServer.stop(router)
  end

  defp start_server(tmp_dir) do
    unique = System.unique_integer([:positive])
    mgr = :"bm-#{unique}"
    sessions = :"ss-#{unique}"

    router =
      start_supervised!(
        {Routing.Router,
         name: nil, config: Config.new(snapshot_path: Path.join(tmp_dir, "r.json"))}
      )

    {:ok, g} = Routing.put_group(router, gid: "ops")
    {:ok, _} = Routing.put_user(router, uid: "u1", username: "alice", secret: "pw", group: g)
    {:ok, _} = Routing.set_smpp_secret(router, "u1", "smpp-secret")
    {:ok, _} = Routing.set_max_bindings(router, "u1", 3)

    {:ok, sup} =
      ServerSupervisor.start_link(
        config: Server.Config.new(enabled: true, port: 0),
        router: router,
        binding_manager: mgr,
        session_supervisor: sessions
      )

    {router, sup, mgr}
  end

  defp listener_port(sup), do: Listener.port(child(sup, Listener))
  defp sessions(sup), do: child(sup, SessionSupervisor)
  defp session_count(sup), do: length(DynamicSupervisor.which_children(sessions(sup)))

  defp child(sup, id) do
    {^id, pid, _, _} = Enum.find(Supervisor.which_children(sup), &(elem(&1, 0) == id))
    pid
  end
end
