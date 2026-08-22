defmodule JasminEx.Routing.FilterTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.User

  setup_all do
    {:ok, group} = Group.new(gid: "ops")
    {:ok, user} = User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)
    {:ok, group: group, user: user}
  end

  test "matches a route only when every filter equals the routable", ctx do
    assert {:ok, user_filter} = Filter.User.new(uid: "u1")
    assert {:ok, dest_filter} = Filter.Destination.new(address: "21200000")
    assert Filter.match?([user_filter, dest_filter], routable(ctx))
  end

  test "does not match when any AND filter misses the routable", ctx do
    assert {:ok, user_filter} = Filter.User.new(uid: "u1")
    assert {:ok, dest_filter} = Filter.Destination.new(address: "99999999")
    refute Filter.match?([user_filter, dest_filter], routable(ctx))
  end

  test "treats an empty filter list as a match for any routable", ctx do
    assert Filter.match?([], routable(ctx, destination: "111"))
    assert Filter.match?([], routable(ctx, destination: "222", tags: ["other"]))
  end

  test "matches TagsAll only when every required tag is present", ctx do
    assert {:ok, tags_filter} = Filter.TagsAll.new(tags: ["vip", "promo"])
    assert Filter.match?([tags_filter], routable(ctx, tags: ["promo", "vip", "extra"]))
    refute Filter.match?([tags_filter], routable(ctx, tags: ["vip"]))
    refute Filter.match?([tags_filter], routable(ctx, tags: []))
  end

  test "does not provide EvalPyFilter or script matching", ctx do
    refute Code.ensure_loaded?(JasminEx.Routing.Filter.EvalPyFilter)
    refute function_exported?(Filter, :eval_py, 1)
    refute function_exported?(Filter, :eval_py, 2)

    script = %{__struct__: JasminEx.Routing.Filter.EvalPyFilter, script: "True"}
    refute Filter.match?([script], routable(ctx))
  end

  defp routable(ctx, overrides \\ []) do
    attrs = [
      user: ctx.user,
      group: ctx.group,
      source: "1616",
      destination: "21200000",
      content: "hello",
      tags: ["vip"]
    ]

    assert {:ok, routable} = Routable.new(Keyword.merge(attrs, overrides))
    routable
  end
end
