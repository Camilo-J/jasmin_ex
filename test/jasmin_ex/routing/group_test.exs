defmodule JasminEx.Routing.GroupTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing.Group

  test "creates an enabled group with an immutable gid" do
    assert {:ok, %Group{gid: "ops", enabled: true} = group} = Group.new(gid: "ops")
    assert {:ok, %Group{gid: "ops", enabled: false}} = Group.put_enabled(group, false)
    assert {:ok, %Group{gid: "ops", enabled: true}} = Group.put_enabled(group, true)
  end

  test "creates a disabled group without changing the gid" do
    assert {:ok, %Group{gid: "ops-2", enabled: false} = group} =
             Group.new(gid: "ops-2", enabled: false)

    assert {:ok, %Group{gid: "ops-2", enabled: true}} = Group.put_enabled(group, true)
  end

  test "rejects invalid gid syntax and non-boolean enabled flags" do
    assert {:error, :invalid_gid} = Group.new(gid: "")
    assert {:error, :invalid_gid} = Group.new(gid: "this-gid-is-too-long")
    assert {:error, :invalid_gid} = Group.new(gid: "ops!")
    assert {:error, :invalid_gid} = Group.new(gid: :ops)
    assert {:error, :invalid_enabled} = Group.new(gid: "ops", enabled: :yes)
    assert {:error, :invalid_enabled} = Group.new(gid: "ops", enabled: nil)
  end

  test "rejects enabled updates that would mutate gid or use an invalid flag" do
    assert {:ok, group} = Group.new(gid: "ops")
    assert {:error, :invalid_enabled} = Group.put_enabled(group, "false")
    assert group.gid == "ops"
  end
end
