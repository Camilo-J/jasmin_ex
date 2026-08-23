defmodule JasminEx.Routing.TelemetryTest do
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  alias JasminEx.Routing
  alias JasminEx.Routing.{Config, ConnectorRef, Filter, Routable, Telemetry}

  @secret "s3cret-pass"
  @drop [
    :username,
    :uid,
    :gid,
    :secret,
    :salt,
    :digest,
    :source,
    :destination,
    :content,
    :tags,
    :path
  ]
  @events for suffix <- [[:mutation], [:snapshot], [:auth], [:resolve]],
              do: [:jasmin_ex, :routing | suffix]

  defmodule InjectedOps do
    @moduledoc false
    defdelegate mkdir_p(path), to: JasminEx.Routing.FileOps
    def write(_path, _data), do: {:error, :eio}
    defdelegate fsync(path), to: JasminEx.Routing.FileOps
    defdelegate rename(from, to), to: JasminEx.Routing.FileOps
    defdelegate chmod(path, mode), to: JasminEx.Routing.FileOps
    defdelegate read(path), to: JasminEx.Routing.FileOps
  end

  setup do
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(id, @events, &__MODULE__.handle_telemetry/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  def handle_telemetry(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end

  test "emit keeps outcomes and drops secrets, identities, and addresses" do
    Telemetry.emit([:mutation], %{}, %{
      outcome: :ok,
      username: "alice",
      secret: @secret,
      uid: "u1",
      gid: "ops",
      salt: "s",
      digest: "d",
      source: "1616",
      destination: "21200000",
      content: "hello",
      tags: ["vip"],
      path: "/tmp/routing.json"
    })

    {%{}, meta} = event([:mutation])
    assert meta.outcome == :ok
    refute_forbidden(meta)
  end

  test "records mutation, auth, and resolve outcomes without PII", %{tmp_dir: tmp_dir} do
    router = start_router(tmp_dir)
    assert {:ok, group} = Routing.put_group(router, gid: "ops")
    {%{}, mutation} = event([:mutation])
    assert mutation.outcome == :ok and is_integer(mutation.revision)
    {%{}, snapshot} = event([:snapshot])
    assert snapshot.outcome == :ok
    refute_forbidden(mutation)
    refute_forbidden(snapshot)

    assert {:ok, user} =
             Routing.put_user(router, uid: "u1", username: "alice", secret: @secret, group: group)

    flush_events()
    assert {:ok, ^user} = Routing.authenticate(router, "alice", @secret)
    {%{}, auth_ok} = event([:auth])
    assert auth_ok.result == :ok
    assert {:error, :invalid_credentials} = Routing.authenticate(router, "alice", "wrong")
    {%{}, auth_bad} = event([:auth])
    assert auth_bad.result == :invalid_credentials
    refute_forbidden(auth_ok)
    refute_forbidden(auth_bad)

    assert {:ok, connector} = ConnectorRef.new("smpp-a")
    assert {:ok, dest} = Filter.Destination.new(address: "21200000")

    assert {:ok, _} =
             Routing.put_route(router,
               kind: :static,
               order: 10,
               connector: connector,
               filters: [dest]
             )

    flush_events()
    assert {:ok, _} = Routing.resolve(router, routable(user, group, "21200000"))
    {%{duration: duration}, resolved} = event([:resolve])
    assert resolved.result == :ok and resolved.order == 10
    assert is_integer(duration) and duration >= 0
    assert {:error, :no_route} = Routing.resolve(router, routable(user, group, "999"))
    {%{duration: _}, missed} = event([:resolve])
    assert missed.result == :no_route and missed.order == nil
    refute_forbidden(resolved)
    refute_forbidden(missed)
  end

  test "snapshot write failure emits outcome without path or payload", %{tmp_dir: tmp_dir} do
    config =
      Config.new(snapshot_path: Path.join(tmp_dir, "routing-v1.json"), file_ops: InjectedOps)

    router = start_supervised!({Routing.Router, [config: config]})
    flush_events()
    assert {:error, :snapshot_failed} = Routing.put_group(router, gid: "ops")
    {%{}, mutation} = event([:mutation])
    assert mutation.outcome == :error and mutation.reason == :snapshot_failed
    {%{}, snapshot} = event([:snapshot])
    assert snapshot.outcome == :snapshot_failed
    refute_forbidden(mutation)
    refute_forbidden(snapshot)
  end

  defp event(suffix) do
    assert_receive {:telemetry, [:jasmin_ex, :routing | ^suffix], measurements, metadata}
    {measurements, metadata}
  end

  defp flush_events do
    receive do
      {:telemetry, _, _, _} -> flush_events()
    after
      0 -> :ok
    end
  end

  defp refute_forbidden(map) do
    refute inspect(map) =~ @secret
    refute Enum.any?(@drop, &Map.has_key?(map, &1))
  end

  defp routable(user, group, destination) do
    {:ok, routable} =
      Routable.new(
        user: user,
        group: group,
        source: "1616",
        destination: destination,
        content: "hello",
        tags: []
      )

    routable
  end

  defp start_router(tmp_dir) do
    path = Path.join(tmp_dir, "routing-v1.json")
    router = start_supervised!({Routing.Router, config: Config.new(snapshot_path: path)})
    flush_events()
    router
  end
end
