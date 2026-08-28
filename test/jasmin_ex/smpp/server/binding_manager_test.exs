defmodule JasminEx.Smpp.Server.BindingManagerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing
  alias JasminEx.Routing.State
  alias JasminEx.Smpp.Server.BindingManager

  test "concurrent limit-1 admits exactly one binding" do
    mgr = start_supervised!(BindingManager)
    parent = self()
    go = make_ref()

    pids =
      Enum.map(1..2, fn _ ->
        spawn_link(fn ->
          send(parent, :ready)
          receive do: (^go -> send(parent, BindingManager.register(mgr, "alice", :tx, self(), 1)))
          Process.sleep(:infinity)
        end)
      end)

    assert_receive :ready
    assert_receive :ready
    Enum.each(pids, &send(&1, go))

    results =
      for _ <- 1..2 do
        receive do
          result when result in [:ok, {:error, :max_bindings_exceeded}] -> result
        after
          1_000 -> flunk("timed out waiting for register")
        end
      end

    assert Enum.frequencies(results) == %{:ok => 1, {:error, :max_bindings_exceeded} => 1}
    assert BindingManager.count(mgr, "alice") == 1
  end

  test "failed SMPP auth never registers; duplicate pid is rejected" do
    mgr = start_supervised!(BindingManager)

    assert {:error, :invalid_credentials} =
             Routing.authenticate_smpp_snapshot(State.new(), "alice", "password")

    assert BindingManager.count(mgr, "alice") == 0
    assert :ok = BindingManager.register(mgr, "alice", :tx, self(), 2)
    assert {:error, :duplicate_pid} = BindingManager.register(mgr, "alice", :rx, self(), 2)
    bob = spawn(fn -> Process.sleep(:infinity) end)
    assert {:error, :max_bindings_exceeded} = BindingManager.register(mgr, "bob", :tx, bob, 0)
  end

  test "DOWN and release free quota exactly once" do
    mgr = start_supervised!(BindingManager)
    pid = spawn(fn -> Process.sleep(:infinity) end)
    ref = Process.monitor(pid)
    assert :ok = BindingManager.register(mgr, "alice", :tx, pid, 1)

    assert {:error, :max_bindings_exceeded} =
             BindingManager.register(mgr, "alice", :rx, self(), 1)

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    assert BindingManager.count(mgr, "alice") == 0
    assert :ok = BindingManager.release(mgr, pid)
    assert :ok = BindingManager.register(mgr, "alice", :tx, self(), 1)
    assert :ok = BindingManager.release(mgr, self())
    assert :ok = BindingManager.release(mgr, self())
    assert BindingManager.count(mgr, "alice") == 0
  end
end
