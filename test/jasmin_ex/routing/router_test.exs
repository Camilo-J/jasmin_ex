defmodule JasminEx.Routing.RouterTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.User

  test "creates user u1/alice in group ops" do
    router = start_router()

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

  test "cascade-deletes users and leaves routes T unchanged" do
    router = start_router()
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

  test "does not publish invalid writes" do
    router = start_router()
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

  test "returns a typed error for a malformed route and keeps Router alive" do
    router = start_router()
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

  test "authenticates eligible users and returns typed failures without secrets" do
    router = start_router()
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

  defp start_router do
    dir = Path.join(System.tmp_dir!(), "jr-#{System.unique_integer([:positive])}")
    config = Config.new(snapshot_path: Path.join(dir, "routing-v1.json"))
    start_supervised!({Routing.Router, name: nil, config: config})
  end
end
