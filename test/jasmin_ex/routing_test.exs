defmodule JasminEx.RoutingTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Routable

  test "resolve returns only a connector ref or inert :no_route" do
    router = start_supervised!({Routing.Router, name: nil})
    assert {:ok, group} = Routing.put_group(router, gid: "ops")

    assert {:ok, user} =
             Routing.put_user(router,
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    assert {:ok, connector} = ConnectorRef.new("smpp-a")
    assert {:ok, dest} = Filter.Destination.new(address: "21200000")

    assert {:ok, _} =
             Routing.put_route(router,
               kind: :static,
               order: 10,
               connector: connector,
               filters: [dest]
             )

    assert {:ok, routable} =
             Routable.new(
               user: user,
               group: group,
               source: "1616",
               destination: "21200000",
               content: "hello",
               tags: []
             )

    before = Routing.snapshot(router)
    assert {:ok, %ConnectorRef{id: "smpp-a"} = resolved} = Routing.resolve(router, routable)
    assert resolved == connector
    assert Routing.snapshot(router) == before

    assert {:ok, miss} =
             Routable.new(
               user: user,
               group: group,
               source: "1616",
               destination: "999",
               content: "hello",
               tags: []
             )

    assert {:error, :no_route} = Routing.resolve(router, miss)
    assert Routing.snapshot(router) == before
    refute inspect(resolved) =~ "s3cret"
  end
end
