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
    assert state.segment_dispatches == %{}
  end

  test "v1_restore_synthesizes_no_billing_activity", %{tmp_dir: dir} do
    user = hd(v1()["users"]) |> Map.merge(%{"balance_minor" => 999, "submit_quota" => 7})
    extras = %{"reservations" => [open_res()], "tombstones" => [stone()], "users" => [user]}
    assert {:ok, state} = Snapshot.restore(write!(dir, Map.merge(v1(), extras)))

    assert {state.users["u1"].balance_minor, state.reservations, state.tombstones,
            state.segment_dispatches} ==
             {nil, %{}, %{}, %{}}
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

  test "v2 and v4 extra ledger keys are ignored as legacy", %{tmp_dir: dir} do
    extras = Map.put(open_res(), "ledger", segment_ledger())
    assert {:ok, v2_state} = Snapshot.restore(write!(dir, %{v2() | "reservations" => [extras]}))
    assert v2_state.reservations["bill-1"].ledger == nil
    assert v2_state.segment_dispatches == %{}

    assert {:ok, v4_state} = Snapshot.restore(write!(dir, %{v4() | "reservations" => [extras]}))
    assert v4_state.reservations["bill-1"].ledger == nil
    assert v4_state.segment_dispatches == %{}
  end

  test "v5 ledger round-trip retains identity, count, and outcomes", %{tmp_dir: dir} do
    config = write!(dir, billed_v5())
    assert %{"version" => 5} = config.snapshot_path |> File.read!() |> :json.decode()
    assert {:ok, state} = Snapshot.restore(config)
    open = state.reservations["bill-1"]
    assert open.ledger.bill_id == "bill-1"
    assert open.ledger.count == 1
    assert open.ledger.unit_price == 100
    assert open.ledger.outcomes == %{1 => :rejected}
    assert open.refundable_minor == 0
    assert :ok = Snapshot.write(state, config)
    assert {:ok, again} = Snapshot.restore(config)
    assert again.reservations["bill-1"].fingerprint == open.fingerprint
    assert again.reservations["bill-1"].ledger.outcomes == %{1 => :rejected}
    assert state.segment_dispatches == %{}
    assert again.segment_dispatches == %{}
  end

  test "v6 dispatch round-trip retains identity, child ids, and hashes", %{tmp_dir: dir} do
    config = write!(dir, billed_v6())
    assert %{"version" => 6} = config.snapshot_path |> File.read!() |> :json.decode()
    assert {:ok, state} = Snapshot.restore(config)
    dispatch = state.segment_dispatches["bill-1"]
    assert dispatch.bill_id == "bill-1"
    assert dispatch.count == 1
    assert dispatch.phase == :planned
    assert dispatch.fingerprint == state.reservations["bill-1"].fingerprint
    assert dispatch.count == state.reservations["bill-1"].ledger.count
    assert Enum.map(dispatch.children, & &1.gateway_id) == ["gw-a"]
    assert hd(dispatch.children).payload_hash == <<2::256>>
    assert hd(dispatch.children).status == :unattempted
    assert :ok = Snapshot.write(state, config)

    assert %{"version" => 6, "segment_dispatches" => [_encoded]} =
             config.snapshot_path |> File.read!() |> :json.decode()

    refute File.read!(config.snapshot_path) =~ "short_message"
    assert {:ok, again} = Snapshot.restore(config)
    assert again.segment_dispatches["bill-1"].children == dispatch.children
    assert again.users["u1"].balance_minor == 300
  end

  test "v1-v5 extra dispatch keys stay empty; v6 missing field fails closed", %{tmp_dir: dir} do
    extras = %{"segment_dispatches" => [segment_dispatch()]}
    assert {:ok, v1_state} = Snapshot.restore(write!(dir, Map.merge(v1(), extras)))
    assert v1_state.segment_dispatches == %{}
    assert {:ok, v5_state} = Snapshot.restore(write!(dir, Map.merge(v5(), extras)))
    assert v5_state.segment_dispatches == %{}

    assert {:error, {:restore_failed, :invalid_state}} =
             Snapshot.restore(write!(dir, Map.put(v5(), "version", 6)))
  end

  test "v6 malformed ownership, ids, digest, outcome, and phase fail closed", %{tmp_dir: dir} do
    Enum.each(
      [
        %{v6() | "segment_dispatches" => "nope"},
        %{billed_v6() | "segment_dispatches" => [Map.delete(segment_dispatch(), "bill_id")]},
        %{billed_v6() | "segment_dispatches" => [Map.delete(segment_dispatch(), "children")]},
        %{billed_v6() | "segment_dispatches" => [Map.delete(segment_dispatch(), "phase")]},
        %{billed_v6() | "segment_dispatches" => [%{segment_dispatch() | "bill_id" => "missing"}]},
        %{
          billed_v6()
          | "segment_dispatches" => [
              %{segment_dispatch() | "fingerprint" => %{"version" => 1, "digest" => "xxxx"}}
            ]
        },
        %{
          billed_v6()
          | "segment_dispatches" => [
              %{
                segment_dispatch()
                | "children" => [
                    %{"gateway_id" => "gw-a", "payload_hash" => "abcd", "status" => "unattempted"}
                  ]
              }
            ]
        },
        %{
          billed_v6()
          | "segment_dispatches" => [
              %{
                segment_dispatch()
                | "count" => 2,
                  "children" => [
                    %{
                      "gateway_id" => "gw-a",
                      "payload_hash" => Base.encode64(<<2::256>>),
                      "status" => "unattempted"
                    },
                    %{
                      "gateway_id" => "gw-a",
                      "payload_hash" => Base.encode64(<<3::256>>),
                      "status" => "unattempted"
                    }
                  ]
              }
            ]
        },
        %{billed_v6() | "segment_dispatches" => [%{segment_dispatch() | "phase" => "closed"}]},
        %{billed_v6() | "segment_dispatches" => [%{segment_dispatch() | "phase" => "accepted"}]},
        %{
          billed_v6()
          | "segment_dispatches" => [
              %{
                segment_dispatch()
                | "children" => [
                    %{
                      "gateway_id" => "gw-a",
                      "payload_hash" => Base.encode64(<<2::256>>),
                      "status" => "accepted"
                    }
                  ]
              }
            ]
        },
        %{billed_v6() | "segment_dispatches" => [segment_dispatch(), segment_dispatch()]},
        billed_v6_two([
          child_json("gw-a", <<2::256>>, "unattempted"),
          child_json("gw-b", <<3::256>>, "claimed")
        ]),
        billed_v6_two([
          child_json("gw-a", <<2::256>>, "claimed"),
          child_json("gw-b", <<3::256>>, "claimed")
        ]),
        billed_v6_two([
          child_json("gw-a", <<2::256>>, "failed"),
          child_json("gw-b", <<3::256>>, "queued")
        ])
        |> put_in(["segment_dispatches", Access.at(0), "phase"], "stopped")
        |> put_in(["segment_dispatches", Access.at(0), "stop_outcome"], "rejected")
      ],
      &assert_closed(dir, &1)
    )
  end

  test "v6 restores helper-reachable queued prefix and single claim", %{tmp_dir: dir} do
    billed =
      billed_v6_two([
        child_json("gw-a", <<2::256>>, "queued"),
        child_json("gw-b", <<3::256>>, "claimed")
      ])

    assert {:ok, state} = Snapshot.restore(write!(dir, billed))
    dispatch = state.segment_dispatches["bill-1"]
    assert dispatch.phase == :dispatching
    assert Enum.map(dispatch.children, & &1.status) == [:queued, :claimed]
    assert state.users["u1"].balance_minor == 300
  end

  test "v6 tombstone-anchored checkpoint restores without changing balances", %{tmp_dir: dir} do
    dispatch = %{segment_dispatch() | "bill_id" => "bill-t"}
    billed = billed_v6() |> Map.put("segment_dispatches", [dispatch])
    assert {:ok, state} = Snapshot.restore(write!(dir, billed))
    assert state.users["u1"].balance_minor == 300
    assert state.users["u1"].submit_quota == 1
    assert state.tombstones["bill-t"].state == :settled_ok
    assert state.segment_dispatches["bill-t"].bill_id == "bill-t"

    assert state.segment_dispatches["bill-t"].fingerprint ==
             state.tombstones["bill-t"].fingerprint

    refute Map.has_key?(state.reservations, "bill-t")
  end

  test "malformed v5 ledger and incoherent money fail closed", %{tmp_dir: dir} do
    Enum.each(
      [
        %{v5() | "reservations" => [Map.put(open_res(), "ledger", "nope")]},
        %{
          v5()
          | "reservations" => [Map.put(open_res(), "ledger", %{segment_ledger() | "count" => 0})]
        },
        %{
          v5()
          | "reservations" => [
              Map.put(open_res(), "ledger", %{segment_ledger() | "bill_id" => "other"})
            ]
        },
        %{
          v5()
          | "reservations" => [
              Map.put(open_res(), "ledger", %{segment_ledger() | "unit_price" => -1})
            ]
        },
        %{
          v5()
          | "reservations" => [
              Map.put(open_res(), "ledger", %{
                segment_ledger()
                | "outcomes" => [%{"index" => 1, "outcome" => "nope"}]
              })
            ]
        },
        %{
          v5()
          | "reservations" => [
              Map.put(open_res(), "ledger", %{
                segment_ledger()
                | "outcomes" => [%{"index" => 2, "outcome" => "rejected"}]
              })
            ]
        },
        %{
          v5()
          | "reservations" => [
              Map.put(open_res(), "ledger", %{
                segment_ledger()
                | "outcomes" => [%{"index" => 1, "outcome" => "rejected"}]
              })
              |> Map.put("refundable_minor", 90)
            ]
        }
      ],
      &assert_closed(dir, &1)
    )
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

  defp v4 do
    user =
      Map.merge(hd(v2()["users"]), %{
        "smpp_credential" => :null,
        "max_bindings" => 0,
        "set_dlr_level" => true,
        "http_set_dlr_method" => true
      })

    Map.merge(v2(), %{"version" => 4, "users" => [user]})
  end

  defp v5 do
    user =
      Map.merge(hd(v2()["users"]), %{
        "smpp_credential" => :null,
        "max_bindings" => 0,
        "set_dlr_level" => true,
        "http_set_dlr_method" => true
      })

    Map.merge(v2(), %{"version" => 5, "users" => [user]})
  end

  defp billed_v5 do
    reservation =
      open_res()
      |> Map.put("refundable_minor", 0)
      |> Map.put("ignored", true)
      |> Map.put("ledger", %{
        segment_ledger()
        | "outcomes" => [%{"index" => 1, "outcome" => "rejected"}]
      })

    v5()
    |> put_in(["users", Access.at(0), "balance_minor"], 300)
    |> put_in(["users", Access.at(0), "submit_quota"], 1)
    |> Map.put("reservations", [reservation])
    |> Map.put("tombstones", [stone()])
  end

  defp segment_ledger do
    %{
      "bill_id" => "bill-1",
      "fingerprint" => @fp,
      "unit_price" => 100,
      "count" => 1,
      "outcomes" => []
    }
  end

  defp v6 do
    Map.merge(v5(), %{"version" => 6, "segment_dispatches" => []})
  end

  defp billed_v6 do
    billed_v5()
    |> Map.put("version", 6)
    |> Map.put("segment_dispatches", [segment_dispatch()])
  end

  defp segment_dispatch do
    %{
      "bill_id" => "bill-1",
      "fingerprint" => @fp,
      "count" => 1,
      "phase" => "planned",
      "stop_outcome" => :null,
      "children" => [
        %{
          "gateway_id" => "gw-a",
          "payload_hash" => Base.encode64(<<2::256>>),
          "status" => "unattempted"
        }
      ]
    }
  end

  defp billed_v6_two(children) do
    reservation =
      open_res()
      |> Map.put("captured_minor", 20)
      |> Map.put("reserved_minor", 180)
      |> Map.put("refundable_minor", 180)
      |> Map.put("ledger", %{segment_ledger() | "count" => 2})

    dispatch = %{
      "bill_id" => "bill-1",
      "fingerprint" => @fp,
      "count" => 2,
      "phase" => "dispatching",
      "stop_outcome" => :null,
      "children" => children
    }

    v5()
    |> Map.put("version", 6)
    |> put_in(["users", Access.at(0), "balance_minor"], 300)
    |> put_in(["users", Access.at(0), "submit_quota"], 1)
    |> Map.put("reservations", [reservation])
    |> Map.put("tombstones", [stone()])
    |> Map.put("segment_dispatches", [dispatch])
  end

  defp child_json(gateway_id, hash, status) do
    %{
      "gateway_id" => gateway_id,
      "payload_hash" => Base.encode64(hash),
      "status" => status
    }
  end
end
