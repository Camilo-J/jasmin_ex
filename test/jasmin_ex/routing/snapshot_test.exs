defmodule JasminEx.Routing.SnapshotTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing

  alias JasminEx.Routing.{
    Config,
    ConnectorRef,
    Credential,
    Filter,
    Group,
    Route,
    Snapshot,
    State,
    User
  }

  test "restart restores equivalent state and corrupt restore fails closed" do
    config = tmp_config("restart")
    state = persisted_state()
    assert :ok = Snapshot.write(state, config)
    assert {:ok, restored} = Snapshot.restore(config)
    assert restored == state
    assert Credential.verify(restored.users["u1"].credential, "s3cret")
    assert {:ok, %File.Stat{mode: dir_mode}} = File.stat(Path.dirname(config.snapshot_path))
    assert {:ok, %File.Stat{mode: file_mode}} = File.stat(config.snapshot_path)
    assert Bitwise.band(dir_mode, 0o777) == 0o700
    assert Bitwise.band(file_mode, 0o777) == 0o600

    File.write!(config.snapshot_path, "not-json")
    assert {:error, {:restore_failed, :invalid_json}} = Snapshot.restore(config)
    File.write!(config.snapshot_path, <<128, 4, 95, 0>>)
    assert {:error, {:restore_failed, :invalid_json}} = Snapshot.restore(config)

    File.write!(
      config.snapshot_path,
      ~s({"version":2,"revision":0,"groups":[],"users":[],"routes":[]})
    )

    assert {:error, {:restore_failed, :unsupported_version}} = Snapshot.restore(config)

    File.write!(
      config.snapshot_path,
      ~s({"version":1,"revision":0,"groups":[],"users":[{"uid":"u1","gid":"missing","username":"a","enabled":true,"credential":{}}],"routes":[]})
    )

    assert {:error, {:restore_failed, :invalid_state}} = Snapshot.restore(config)
  end

  test "router writes before publish, injected I/O fails closed, missing file is empty" do
    Enum.each([:write, :fsync, :rename], fn op ->
      config = tmp_config("fail-#{op}", __MODULE__.InjectedOps)
      router = start_supervised!({Routing.Router, [config: config]}, id: {:fail, op})
      before = Routing.snapshot(router)
      assert {:error, :snapshot_failed} = Routing.put_group(router, gid: "ops")
      assert Routing.snapshot(router) == before
      refute File.exists?(config.snapshot_path)
    end)

    router = start_supervised!({Routing.Router, [config: tmp_config("invalid")]}, id: :invalid)
    before = Routing.snapshot(router)
    assert {:error, :invalid_gid} = Routing.put_group(router, gid: "bad!")
    assert Routing.snapshot(router) == before

    missing = tmp_config("missing")
    assert {:ok, %State{groups: %{}} = empty} = Snapshot.restore(missing)
    assert empty == State.new()
    router = start_supervised!({Routing.Router, [config: missing]}, id: :missing)
    assert Routing.snapshot(router) == State.new()
    assert {:ok, group} = Routing.put_group(router, gid: "ops")
    assert Routing.snapshot(router).groups["ops"] == group
    assert Routing.snapshot(router).revision == 1
    assert File.exists?(missing.snapshot_path)
    restarted = start_supervised!({Routing.Router, [config: missing]}, id: :restarted)
    assert Routing.snapshot(restarted).groups["ops"] == group
    corrupt = tmp_config("startup-corrupt")
    File.mkdir_p!(Path.dirname(corrupt.snapshot_path))
    File.write!(corrupt.snapshot_path, "not-json")

    assert {:error, {{:restore_failed, :invalid_json}, _}} =
             start_supervised({Routing.Router, [config: corrupt]}, id: :corrupt)
  end

  defp persisted_state do
    {:ok, group} = Group.new(gid: "ops")
    {:ok, user} = User.new(uid: "u1", username: "alice", secret: "s3cret", group: group)
    {:ok, connector} = ConnectorRef.new("smpp-t")
    {:ok, dest} = Filter.Destination.new(address: "21200000")
    {:ok, route} = Route.new(kind: :static, order: 10, connector: connector, filters: [dest])
    {:ok, state} = State.put_group(State.new(), group)
    {:ok, state} = State.put_user(state, user)
    {:ok, state} = State.put_route(state, route)
    %{state | revision: 3}
  end

  defp tmp_config(label, file_ops \\ nil) do
    dir = Path.join(System.tmp_dir!(), "jr-#{label}-#{System.unique_integer([:positive])}")
    Config.new(snapshot_path: Path.join(dir, "routing-v1.json"), file_ops: file_ops)
  end
end

defmodule JasminEx.Routing.SnapshotTest.InjectedOps do
  @moduledoc false
  alias JasminEx.Routing.FileOps
  def mkdir_p(path), do: FileOps.mkdir_p(path)
  def write(path, data), do: maybe(:write, path, fn -> FileOps.write(path, data) end)
  def fsync(path), do: maybe(:fsync, path, fn -> FileOps.fsync(path) end)
  def rename(from, to), do: maybe(:rename, to, fn -> FileOps.rename(from, to) end)
  def chmod(path, mode), do: FileOps.chmod(path, mode)
  def read(path), do: FileOps.read(path)

  defp maybe(op, path, fun),
    do: if(String.contains?(path, "fail-#{op}"), do: {:error, :eio}, else: fun.())
end
