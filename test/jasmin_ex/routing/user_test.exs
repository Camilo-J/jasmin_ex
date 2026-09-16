defmodule JasminEx.Routing.UserTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing
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

  test "SMPP secret is hashed; password, wrong, unknown, or missing secret fail typed" do
    assert {:ok, group} = Group.new(gid: "ops")
    assert {:ok, user} = User.new(uid: "u1", username: "alice", secret: "password", group: group)
    assert {:ok, state} = State.put_group(State.new(), group)
    assert {:ok, state} = State.put_user(state, user)

    assert {:error, :invalid_credentials} =
             Routing.authenticate_smpp_snapshot(state, "alice", "password")

    assert {:error, :invalid_secret} = State.set_smpp_secret(state, "u1", "")
    assert {:error, :unknown_user} = State.set_smpp_secret(state, "missing", "smpp-secret")
    assert {:ok, state, user} = State.set_smpp_secret(state, "u1", "smpp-secret")
    assert Credential.verify(user.smpp_credential, "smpp-secret")
    refute Credential.verify(user.smpp_credential, "password")
    refute inspect(user) =~ "smpp-secret"
    refute inspect(user) =~ "password"

    assert {:ok, %User{uid: "u1"}} =
             Routing.authenticate_smpp_snapshot(state, "alice", "smpp-secret")

    before = state

    assert {:error, :invalid_credentials} =
             Routing.authenticate_smpp_snapshot(state, "alice", "password")

    assert {:error, :invalid_credentials} =
             Routing.authenticate_smpp_snapshot(state, "alice", "wrong")

    assert {:error, :invalid_credentials} =
             Routing.authenticate_smpp_snapshot(state, "unknown", "smpp-secret")

    assert state == before
    assert {:ok, %User{uid: "u1"}} = Routing.authenticate_snapshot(state, "alice", "password")
  end

  test "new and existing users default DLR permissions to true" do
    assert {:ok, group} = Group.new(gid: "ops")

    assert {:ok, %User{} = user} =
             User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)

    assert user.set_dlr_level == true
    assert user.http_set_dlr_method == true
  end

  test "explicit false DLR permissions survive User construction" do
    assert {:ok, group} = Group.new(gid: "ops")

    assert {:ok, %User{} = user} =
             User.new(
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               set_dlr_level: false,
               http_set_dlr_method: false
             )

    assert user.set_dlr_level == false
    assert user.http_set_dlr_method == false
  end

  test "invalid non-boolean DLR permissions are rejected" do
    assert {:ok, group} = Group.new(gid: "ops")

    assert {:error, :invalid_dlr_permission} =
             User.new(
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               set_dlr_level: :no
             )

    assert {:error, :invalid_dlr_permission} =
             User.new(
               uid: "u1",
               username: "alice",
               secret: "s3cret",
               group: group,
               http_set_dlr_method: "false"
             )

    assert {:ok, user} = User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)
    assert {:error, :invalid_dlr_permission} = User.set_dlr_level(user, :no)
    assert {:error, :invalid_dlr_permission} = User.set_http_set_dlr_method(user, 0)
    assert user.set_dlr_level == true
    assert user.http_set_dlr_method == true
    assert {:ok, %User{set_dlr_level: false}} = User.set_dlr_level(user, false)
    assert {:ok, %User{http_set_dlr_method: false}} = User.set_http_set_dlr_method(user, false)
  end

  test "invalid max_bindings fails typed and leaves the prior quota unchanged" do
    assert {:ok, group} = Group.new(gid: "ops")
    assert {:ok, user} = User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)
    assert {:ok, %User{max_bindings: 2} = user} = User.set_max_bindings(user, 2)
    assert {:error, :invalid_max_bindings} = User.set_max_bindings(user, -1)
    assert user.max_bindings == 2
    assert {:ok, %User{max_bindings: 0}} = User.set_max_bindings(user, 0)
    assert {:ok, state} = State.put_group(State.new(), group)
    assert {:ok, state} = State.put_user(state, user)
    before = state
    assert {:error, :invalid_max_bindings} = State.set_max_bindings(state, "u1", :nope)
    assert {:error, :unknown_user} = State.set_max_bindings(state, "missing", 1)
    assert state == before
  end
end
