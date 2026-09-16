defmodule JasminEx.Routing.RouterTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.User

  test "creates user u1/alice in group ops", %{tmp_dir: tmp_dir} do
    router = start_router(tmp_dir)

    assert {:ok, %Group{gid: "ops", enabled: true} = group} =
             Routing.put_group(router, gid: "ops")

    assert {:ok, %User{uid: "u1", username: "alice", gid: "ops"} = user} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    snap = Routing.snapshot(router)
    assert snap.groups["ops"] == group
    assert snap.users["u1"] == user
  end

  test "cascade-deletes users and leaves routes T unchanged", %{tmp_dir: tmp_dir} do
    router = start_router(tmp_dir)
    assert {:ok, group} = Routing.put_group(router, gid: "ops")

    assert {:ok, _user} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    assert {:ok, connector} = ConnectorRef.new("smpp-t")
    assert {:ok, route} = Route.new(kind: :static, order: 10, connector: connector, filters: [])

    assert {:ok, ^route} =
             Routing.put_route(router,
               kind: :static,
               order: 10,
               connector: connector,
               filters: []
             )

    routes = Routing.snapshot(router).routes
    assert {:ok, "ops"} = Routing.delete_group(router, "ops")
    snap = Routing.snapshot(router)
    assert snap.groups == %{}
    assert snap.users == %{}
    assert snap.routes == routes
  end

  test "does not publish invalid writes", %{tmp_dir: tmp_dir} do
    router = start_router(tmp_dir)
    assert {:ok, _group} = Routing.put_group(router, gid: "ops")
    before = Routing.snapshot(router)
    assert {:error, :invalid_gid} = Routing.put_group(router, gid: "bad!")

    assert {:error, :unknown_group} =
             Routing.put_user(router, uid: "u1", username: "alice", secret: "s3cret")

    assert {:error, :invalid_order} =
             Routing.put_route(router,
               kind: :static,
               order: 0,
               connector: elem(ConnectorRef.new("smpp-t"), 1),
               filters: []
             )

    assert {:error, :unknown_group} = Routing.delete_group(router, "missing")
    assert Routing.snapshot(router) == before
  end

  test "returns a typed error for a malformed route and keeps Router alive", %{tmp_dir: tmp_dir} do
    router = start_router(tmp_dir)
    malformed = %ConnectorRef{type: :smpp_client, id: ""}
    script = %{__struct__: JasminEx.Routing.Filter.EvalPyFilter, script: "True"}

    assert {:error, reason} =
             Routing.put_route(router,
               kind: :static,
               order: 10,
               connector: malformed,
               filters: [script]
             )

    assert reason in [:invalid_connector, :invalid_filter]
    assert Process.alive?(router)
    assert %Routing.State{} = Routing.snapshot(router)

    {:ok, connector} = ConnectorRef.new("smpp-t")

    assert {:error, :invalid_filter} =
             Routing.put_route(router,
               kind: :static,
               order: 10,
               connector: connector,
               filters: [script]
             )

    assert Process.alive?(router)
    assert %Routing.State{} = Routing.snapshot(router)
  end

  test "authenticates eligible users and returns typed failures without secrets", %{
    tmp_dir: tmp_dir
  } do
    router = start_router(tmp_dir)
    assert {:ok, group} = Routing.put_group(router, gid: "ops")

    assert {:ok, user} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    assert {:ok, ^user} = Routing.authenticate(router, "alice", "s3cret")
    assert {:error, :invalid_credentials} = Routing.authenticate(router, "alice", "wrong")
    assert {:error, :invalid_credentials} = Routing.authenticate(router, "nobody", "s3cret")

    assert {:ok, _} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               enabled: false
             )

    assert {:error, :user_disabled} = Routing.authenticate(router, "alice", "s3cret")

    assert {:ok, _} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               enabled: true
             )

    assert {:ok, _} = Routing.put_group(router, gid: "ops", enabled: false)
    assert {:error, :group_disabled} = result = Routing.authenticate(router, "alice", "s3cret")
    refute inspect(result) =~ "s3cret"
    refute inspect(Routing.snapshot(router)) =~ "s3cret"
  end

  test "set_max_bindings persists, rejects invalid input, and treats 0 as disable", %{
    tmp_dir: tmp_dir
  } do
    router = start_router(tmp_dir)
    assert {:ok, group} = Routing.put_group(router, gid: "ops")

    assert {:ok, _} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    assert {:ok, %User{max_bindings: 2}} = Routing.set_max_bindings(router, "u1", 2)
    before = Routing.snapshot(router)
    assert {:error, :invalid_max_bindings} = Routing.set_max_bindings(router, "u1", -1)
    assert Routing.snapshot(router) == before
    assert {:ok, %User{max_bindings: 0}} = Routing.set_max_bindings(router, "u1", 0)
    assert {:ok, _} = Routing.set_smpp_secret(router, "u1", "smpp-secret")
    assert {:ok, _} = Routing.authenticate_smpp(router, "alice", "smpp-secret")
    assert {:error, :invalid_credentials} = Routing.authenticate_smpp(router, "alice", "s3cret")
    refute inspect(Routing.snapshot(router)) =~ "smpp-secret"
  end

  test "DLR permission setters persist before publish and reject invalid input", %{
    tmp_dir: tmp_dir
  } do
    router = start_router(tmp_dir)
    assert {:ok, group} = Routing.put_group(router, gid: "ops")

    assert {:ok, %User{set_dlr_level: true, http_set_dlr_method: true}} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    assert {:ok, %User{set_dlr_level: false}} = Routing.set_dlr_level(router, "u1", false)
    before = Routing.snapshot(router)
    assert {:error, :invalid_dlr_permission} = Routing.set_dlr_level(router, "u1", :no)
    assert Routing.snapshot(router) == before

    assert {:ok, %User{http_set_dlr_method: false}} =
             Routing.set_http_set_dlr_method(router, "u1", false)

    before = Routing.snapshot(router)

    assert {:error, :invalid_dlr_permission} =
             Routing.set_http_set_dlr_method(router, "u1", "false")

    assert Routing.snapshot(router) == before
    assert {:error, :unknown_user} = Routing.set_dlr_level(router, "missing", false)
    restarted = start_supervised!({Routing.Router, [config: router_config(tmp_dir)]}, id: :dlr)
    assert Routing.snapshot(restarted).users["u1"].set_dlr_level == false
    assert Routing.snapshot(restarted).users["u1"].http_set_dlr_method == false
  end

  defp start_router(tmp_dir) do
    start_supervised!({Routing.Router, name: nil, config: router_config(tmp_dir)})
  end

  defp router_config(tmp_dir) do
    Config.new(snapshot_path: Path.join(tmp_dir, "routing-v1.json"))
  end
end
