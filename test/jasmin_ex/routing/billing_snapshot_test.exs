Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Routing.BillingSnapshotTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  alias JasminEx.Billing.FakeClock
  alias JasminEx.Routing.{Config, Snapshot}

  test "v1_restore_applies_exact_billing_defaults", %{tmp_dir: dir} do
    assert {:ok, state} = Snapshot.restore(write!(dir, v1()))
    assert {state.users["u1"].balance_minor, state.users["u1"].submit_quota} == {nil, nil}

    assert {state.routes.routes[10].rate_minor, state.routes.routes[10].precharge_percent} ==
             {0, 0}

    assert state.reservations == %{} and state.tombstones == %{}
  end

  test "v1_restore_synthesizes_no_billing_activity", %{tmp_dir: dir} do
    user = hd(v1()["users"]) |> Map.merge(%{"balance_minor" => 999, "submit_quota" => 7})
    extras = %{"reservations" => [open_res()], "tombstones" => [stone()], "users" => [user]}
    assert {:ok, state} = Snapshot.restore(write!(dir, Map.merge(v1(), extras)))

    assert {state.users["u1"].balance_minor, state.reservations, state.tombstones} ==
             {nil, %{}, %{}}
  end

  test "malformed_v2_billing_fields_fail_closed", %{tmp_dir: dir} do
    Enum.each(
      [
        Map.put(v2(), "reservations", "nope"),
        %{v2() | "reservations" => [Map.delete(open_res(), "uid")]},
        %{v2() | "reservations" => [%{open_res() | "fingerprint" => "bad"}]},
        bad_digest(%{}),
        bad_digest([]),
        bad_digest(1),
        bad_digest(:null)
      ],
      &assert_closed(dir, &1)
    )
  end

  test "out_of_range_v2_billing_fields_fail_closed", %{tmp_dir: dir} do
    Enum.each(
      [
        put_in(v2(), ["users", Access.at(0), "balance_minor"], -1),
        put_in(v2(), ["routes", Access.at(0), "precharge_percent"], 101),
        %{v2() | "reservations" => [%{open_res() | "bill_id" => ""}]},
        %{v2() | "tombstones" => [%{stone() | "state" => "open"}]}
      ],
      &assert_closed(dir, &1)
    )
  end

  test "v2 billing state round-trip", %{tmp_dir: dir} do
    config = write!(dir, billed_v2())
    assert %{"version" => 2} = config.snapshot_path |> File.read!() |> :json.decode()
    assert {:ok, state} = Snapshot.restore(config)
    open = state.reservations["bill-1"]
    assert {state.users["u1"].balance_minor, state.users["u1"].submit_quota} == {300, 1}

    assert {state.routes.routes[10].rate_minor, state.routes.routes[10].precharge_percent} ==
             {100, 10}

    assert state.tombstones["bill-t"].state == :settled_ok

    assert {open.uid, open.captured_minor, open.reserved_minor, open.refundable_minor,
            open.wall_deadline_ms} ==
             {"u1", 10, 90, 90, 2_000}

    assert :ok = Snapshot.write(state, config)
    assert {:ok, again} = Snapshot.restore(config)
    assert again.reservations["bill-1"].fingerprint == open.fingerprint
    assert again.tombstones["bill-t"].fingerprint == state.tombstones["bill-t"].fingerprint
  end

  test "restart deadline rehydration uses wall remainder", %{tmp_dir: dir} do
    write!(dir, billed_v2())
    later = cfg(dir, {FakeClock, FakeClock.new(wall_ms: 1_500, monotonic_ms: 50)})
    assert {:ok, state} = Snapshot.restore(later)
    assert state.reservations["bill-1"].wall_deadline_ms == 2_000
    assert state.reservations["bill-1"].monotonic_deadline_ms == 550
  end

  defp clock, do: {FakeClock, FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)}

  defp cfg(dir, clock \\ clock()),
    do: Config.new(snapshot_path: Path.join(dir, "routing.json"), clock: clock)

  defp write!(dir, map) do
    config = cfg(dir)
    File.mkdir_p!(Path.dirname(config.snapshot_path))
    File.write!(config.snapshot_path, map |> :json.encode() |> IO.iodata_to_binary())
    config
  end

  defp assert_closed(dir, map) do
    assert {:error, {:restore_failed, :invalid_state}} = Snapshot.restore(write!(dir, map))
  end

  defp v1 do
    v2()
    |> Map.drop(["reservations", "tombstones"])
    |> Map.put("version", 1)
    |> update_in(["users", Access.at(0)], &Map.drop(&1, ["balance_minor", "submit_quota"]))
    |> update_in(["routes", Access.at(0)], &Map.drop(&1, ["rate_minor", "precharge_percent"]))
  end

  defp billed_v2 do
    v2()
    |> put_in(["users", Access.at(0), "balance_minor"], 300)
    |> put_in(["users", Access.at(0), "submit_quota"], 1)
    |> Map.put("reservations", [open_res()])
    |> Map.put("tombstones", [stone()])
  end

  @v2_json ~s({"version":2,"revision":4,"groups":[{"gid":"ops","enabled":true}],"users":[{"uid":"u1","gid":"ops","username":"alice","enabled":true,"credential":{"algorithm":"pbkdf2-hmac-sha256-v1","iterations":600000,"salt":"AAAAAAAAAAAAAAAAAAAAAA==","digest":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},"balance_minor":500,"submit_quota":3}],"routes":[{"kind":"static","order":10,"connector":{"type":"smpp_client","id":"smpp-t"},"filters":[],"rate_minor":100,"precharge_percent":10}],"reservations":[],"tombstones":[]})
  @fp %{"version" => 1, "digest" => Base.encode64(<<1::256>>)}

  defp v2, do: :json.decode(@v2_json)

  defp open_res do
    %{
      "bill_id" => "bill-1",
      "uid" => "u1",
      "fingerprint" => @fp,
      "state" => "open",
      "captured_minor" => 10,
      "reserved_minor" => 90,
      "refundable_minor" => 90,
      "wall_deadline_ms" => 2_000
    }
  end

  defp stone, do: %{"bill_id" => "bill-t", "fingerprint" => @fp, "state" => "settled_ok"}

  defp bad_digest(digest) do
    %{v2() | "reservations" => [Map.put(open_res(), "fingerprint", %{@fp | "digest" => digest})]}
  end
end
