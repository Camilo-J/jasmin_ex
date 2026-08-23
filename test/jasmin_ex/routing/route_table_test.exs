defmodule JasminEx.Routing.RouteTableTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.RouteTable
  alias JasminEx.Routing.User

  setup_all do
    {:ok, group} = Group.new(gid: "ops")
    {:ok, user} = User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)
    {:ok, dest} = Filter.Destination.new(address: "21200000")
    {:ok, other} = Filter.Destination.new(address: "99999999")
    {:ok, smpp_a} = ConnectorRef.new("smpp-a")
    {:ok, smpp_b} = ConnectorRef.new("smpp-b")
    {:ok, smpp_default} = ConnectorRef.new("smpp-default")

    {:ok,
     group: group,
     user: user,
     dest: dest,
     other: other,
     smpp_a: smpp_a,
     smpp_b: smpp_b,
     smpp_default: smpp_default}
  end

  test "keeps unique orders and replaces a route published at the same order", ctx do
    assert {:ok, first} = static_route(10, ctx.smpp_a, [ctx.dest])
    assert {:ok, second} = static_route(20, ctx.smpp_b, [ctx.other])
    assert {:ok, replacement} = static_route(10, ctx.smpp_b, [ctx.other])
    table = RouteTable.new()
    assert {:ok, table} = RouteTable.put(table, first)
    assert {:ok, table} = RouteTable.put(table, second)
    assert table.routes[10].connector == ctx.smpp_a
    assert table.routes[20].connector == ctx.smpp_b
    assert {:ok, table} = RouteTable.put(table, replacement)
    assert table.routes[10].connector == ctx.smpp_b
    assert map_size(table.routes) == 2
  end

  test "rejects a static route published at order 0", ctx do
    assert {:ok, invalid} = static_route(0, ctx.smpp_a, [ctx.dest])
    assert {:error, :invalid_order} = RouteTable.put(RouteTable.new(), invalid)
  end

  test "rejects a default route published outside order 0", ctx do
    assert {:ok, default_ten} =
             Route.new(kind: :default, order: 10, connector: ctx.smpp_default, filters: [])

    assert {:error, :invalid_order} = RouteTable.put(RouteTable.new(), default_ten)

    negative = %Route{kind: :default, order: -1, connector: ctx.smpp_default, filters: []}
    assert {:error, :invalid_order} = RouteTable.put(RouteTable.new(), negative)
  end

  test "rejects a connector whose id fails the ConnectorRef contract" do
    malformed = %ConnectorRef{type: :smpp_client, id: ""}

    assert {:error, :invalid_connector} =
             Route.new(kind: :static, order: 10, connector: malformed, filters: [])

    assert {:error, :invalid_connector} =
             Route.new(
               kind: :static,
               order: 10,
               connector: %ConnectorRef{type: :smpp_client, id: 123},
               filters: []
             )
  end

  test "rejects an unknown filter struct term" do
    {:ok, connector} = ConnectorRef.new("smpp-a")
    script = %{__struct__: JasminEx.Routing.Filter.EvalPyFilter, script: "True"}

    assert {:error, :invalid_filter} = static_route(10, connector, [script])

    assert {:error, :invalid_filter} =
             static_route(10, connector, [%Filter.User{uid: :not_a_binary}])
  end

  test "resolves the first matching static route in descending order", ctx do
    assert {:ok, high} = static_route(20, ctx.smpp_a, [ctx.dest])
    assert {:ok, low} = static_route(10, ctx.smpp_b, [ctx.dest])
    table = RouteTable.new()
    assert {:ok, table} = RouteTable.put(table, low)
    assert {:ok, table} = RouteTable.put(table, high)
    assert {:ok, connector} = RouteTable.resolve(table, routable(ctx))
    assert connector == ctx.smpp_a
  end

  test "falls back to the default route at order 0", ctx do
    assert {:ok, static} = static_route(10, ctx.smpp_a, [ctx.other])
    assert {:ok, default} = default_route(ctx.smpp_default)
    table = RouteTable.new()
    assert {:ok, table} = RouteTable.put(table, static)
    assert {:ok, table} = RouteTable.put(table, default)
    assert {:ok, connector} = RouteTable.resolve(table, routable(ctx))
    assert connector == ctx.smpp_default
  end

  test "returns inert :no_route when nothing matches and no default exists", ctx do
    assert {:ok, static} = static_route(10, ctx.smpp_a, [ctx.other])
    assert {:ok, table} = RouteTable.put(RouteTable.new(), static)
    before = table
    assert {:error, :no_route} = RouteTable.resolve(table, routable(ctx))
    assert table == before
  end

  defp static_route(order, connector, filters),
    do: Route.new(kind: :static, order: order, connector: connector, filters: filters)

  defp default_route(connector),
    do: Route.new(kind: :default, order: 0, connector: connector, filters: [])

  defp routable(ctx) do
    assert {:ok, routable} =
             Routable.new(
               user: ctx.user,
               group: ctx.group,
               source: "1616",
               destination: "21200000",
               content: "hello",
               tags: ["vip"]
             )

    routable
  end
end
