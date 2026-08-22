defmodule JasminEx.Routing.UserTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing.Credential
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.State
  alias JasminEx.Routing.User

  test "hashes the secret and binds an existing group gid" do
    assert {:ok, group} = Group.new(gid: "ops")

    assert {:ok, %User{} = user} =
             User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)

    assert user.uid == "u1"
    assert user.gid == "ops"
    assert user.username == "alice"
    assert user.enabled == true
    assert %Credential{algorithm: "pbkdf2-hmac-sha256-v1", iterations: 600_000} = user.credential
    assert Credential.verify(user.credential, "s3cret")
    refute inspect(user) =~ "s3cret"
    refute inspect(user) =~ inspect(user.credential.digest)
  end

  test "rejects users without an existing group or with invalid identity fields" do
    assert {:ok, group} = Group.new(gid: "ops")

    assert {:error, :unknown_group} =
             User.new(uid: "u1", username: "alice", secret: "s3cret", group: nil)

    assert {:error, :unknown_group} =
             User.new(uid: "u1", username: "alice", secret: "s3cret")

    assert {:error, :invalid_uid} =
             User.new(
               uid: "this-uid-is-too-long",
               username: "alice",
               secret: "s3cret",
               group: group
             )

    assert {:error, :invalid_username} =
             User.new(uid: "u1", username: "alice-is-too-long", secret: "s3cret", group: group)

    assert {:error, :invalid_secret} =
             User.new(uid: "u1", username: "alice", secret: "", group: group)

    assert {:error, :invalid_enabled} =
             User.new(
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               enabled: :no
             )
  end

  test "accepts a user into state only when the group exists and the username is unique" do
    assert {:ok, group} = Group.new(gid: "ops")
    assert {:ok, state} = State.put_group(State.new(), group)

    assert {:ok, alice} =
             User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)

    assert {:ok, %State{} = state} = State.put_user(state, alice)
    assert %User{uid: "u1", username: "alice"} = state.users["u1"]

    assert {:ok, duplicate} =
             User.new(uid: "u2", username: "alice", secret: "other", group: group)

    assert {:error, :duplicate_username} = State.put_user(state, duplicate)
    assert {:error, :unknown_group} = State.put_user(State.new(), alice)
  end
end
