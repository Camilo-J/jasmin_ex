defmodule JasminEx.Billing.QueriesTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Queries
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.Router

  describe "rate quote" do
    test "returns the winning route rate and leaves billing state unchanged", %{tmp_dir: tmp_dir} do
      {router, routable} = start_quoted_router(tmp_dir, rate_minor: 100)
      {:ok, _} = Routing.admit(router, admission(rate_minor: 100))
      snapshot = Routing.snapshot(router)
      before = billing_view(snapshot)

      assert {:ok, 100} = Queries.quote(snapshot, routable)

      assert billing_view(Routing.snapshot(router)) == before
    end

    test "returns a different winning rate without mutating billing state", %{tmp_dir: tmp_dir} do
      {router, routable} = start_quoted_router(tmp_dir, rate_minor: 250)
      {:ok, _} = Routing.admit(router, admission(rate_minor: 250))
      snapshot = Routing.snapshot(router)
      before = billing_view(snapshot)

      assert {:ok, 250} = Queries.quote(snapshot, routable)

      assert billing_view(Routing.snapshot(router)) == before
    end

    test "missing route is typed and leaves billing state unchanged", %{tmp_dir: tmp_dir} do
      {router, _routable} = start_quoted_router(tmp_dir, rate_minor: 100)
      {:ok, _} = Routing.admit(router, admission(rate_minor: 100))
      snapshot = Routing.snapshot(router)
      before = billing_view(snapshot)
      miss = routable(snapshot, destination: "999")

      assert {:error, :no_route} = Queries.quote(snapshot, miss)

      assert billing_view(Routing.snapshot(router)) == before
    end
  end

  describe "balance read" do
    test "returns the finite balance and leaves billing state unchanged", %{tmp_dir: tmp_dir} do
      {router, _routable} = start_quoted_router(tmp_dir, balance_minor: 500, submit_quota: 3)
      {:ok, _} = Routing.admit(router, admission(rate_minor: 100))
      snapshot = Routing.snapshot(router)
      before = billing_view(snapshot)

      assert {:ok, 400} = Queries.balance(snapshot, "u1")

      assert billing_view(Routing.snapshot(router)) == before
    end

    test "returns unlimited for a nil balance without mutating billing state", %{tmp_dir: tmp_dir} do
      {router, _routable} = start_quoted_router(tmp_dir, balance_minor: nil, submit_quota: nil)

      snapshot = Routing.snapshot(router)
      before = billing_view(snapshot)

      assert {:ok, :unlimited} = Queries.balance(snapshot, "u1")

      assert billing_view(Routing.snapshot(router)) == before
    end

    test "unknown user is typed and leaves billing state unchanged", %{tmp_dir: tmp_dir} do
      {router, _routable} = start_quoted_router(tmp_dir, balance_minor: 500, submit_quota: 3)
      {:ok, _} = Routing.admit(router, admission(rate_minor: 100))
      snapshot = Routing.snapshot(router)
      before = billing_view(snapshot)

      assert {:error, :unknown_user} = Queries.balance(snapshot, "missing")

      assert billing_view(Routing.snapshot(router)) == before
    end
  end

  defp start_quoted_router(tmp_dir, opts) do
    config = Config.new(snapshot_path: Path.join(tmp_dir, "routing.json"))
    router = start_supervised!({Router, name: nil, config: config})
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
        rate_minor: Keyword.get(opts, :rate_minor, 100),
        precharge_percent: 10
      )

    snapshot = Routing.snapshot(router)
    {router, routable(snapshot, destination: "21200000")}
  end

  defp routable(snapshot, opts) do
    user = snapshot.users["u1"]
    group = snapshot.groups[user.gid]

    {:ok, routable} =
      Routable.new(
        user: user,
        group: group,
        source: "1616",
        destination: Keyword.get(opts, :destination, "21200000"),
        content: "hello",
        tags: []
      )

    routable
  end

  defp admission(opts) do
    {:ok, bill} =
      Bill.new(
        bill_id: "bill-1",
        uid: "u1",
        route_order: 10,
        rate_minor: Keyword.get(opts, :rate_minor, 100),
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
