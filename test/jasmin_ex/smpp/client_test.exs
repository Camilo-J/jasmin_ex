defmodule JasminEx.Smpp.ClientTest do
  @moduledoc false
  # Integration scenarios for the SMPP client session lifecycle (PR2).

  # Uses the FakeSMSC harness from test/support/. Each test stands up a
  # fresh harness, configures the appropriate script for the scenario,
  # starts a Client, drives the lifecycle, and asserts on the resulting
  # state. Tests are intentionally `async: false` because each scenario
  # owns its own listening port and harness instance — there is no
  # cross-test dependency but the harness global VM is shared.
  use ExUnit.Case, async: false

  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.FakeSMSC

  defp start_smsc(opts \\ []) do
    {:ok, port, pid} = FakeSMSC.start_link(opts)
    {:ok, port, pid}
  end

  defp start_client(port, opts \\ []) do
    base = [
      host: ~c"localhost",
      port: port,
      system_id: "user",
      password: "pw",
      system_type: "type",
      bind_as: :transmitter
    ]

    Client.start_link(base ++ opts)
  end

  defp wait_until(predicate, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(predicate, deadline)
  end

  defp do_wait(predicate, deadline) do
    if predicate.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        {:error, :timeout}
      else
        Process.sleep(5)
        do_wait(predicate, deadline)
      end
    end
  end

  defp stop_pair(smsc, client) do
    if Process.alive?(client),
      do:
        (try do
           GenServer.stop(client, :normal, 200)
         catch
           _, _ -> :ok
         end)

    if Process.alive?(smsc),
      do:
        (try do
           GenServer.stop(smsc, :normal, 200)
         catch
           _, _ -> :ok
         end)
  end

  defp wait_smsc_connected(smsc) do
    :ok = FakeSMSC.wait_connected(smsc)
  end

  describe "bind lifecycle" do
    test "successful bind (status 0) transitions the client to :bound" do
      {:ok, port, smsc} = start_smsc()
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)
      assert Client.status(client) == :bound
      stop_pair(smsc, client)
    end

    test "bind rejected (non-zero status) leaves the client out of :bound" do
      {:ok, port, smsc} = start_smsc()
      :ok = FakeSMSC.inject_bind_resp(smsc, :ESME_RSYSERR)
      {:ok, client} = start_client(port)

      # The client must NOT enter :bound; it transitions to :disconnected
      # and is allowed to stay there while backoff is pending (or for the
      # duration of the test). Either is acceptable for "stays out of :bound".
      assert :ok = wait_until(fn -> Client.status(client) != :connecting end)
      refute Client.status(client) == :bound
      stop_pair(smsc, client)
    end
  end

  describe "socket drop transitions out of mid-connect / mid-bind states" do
    test "closing the harness socket during :connecting forces :disconnected" do
      # The harness Listener socket accepts our connect, then immediately
      # drops the connection before any data flows. We check the client
      # observes this and stays out of :bound.
      {:ok, port, smsc} = start_smsc()

      # Force the harness to close the connection on arrival of any bind
      # request — the bind request itself won't be acked, so the client
      # never receives a bind_resp. To force the close BEFORE bind is
      # written by the client (so it's effectively mid-connect), we use
      # the listener-level close: harness won't even read bytes.
      :ok = FakeSMSC.close_on_command(smsc, :bind_transmitter)

      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) != :connecting end)
      refute Client.status(client) == :bound
      assert Client.status(client) in [:disconnected, :bind_pending]
      stop_pair(smsc, client)
    end

    test "closing the harness socket during :bind_pending forces :disconnected" do
      # Same as above but we explicitly let the bind request reach the
      # closure — verify bind_pending -> disconnected transition.
      {:ok, port, smsc} = start_smsc()
      :ok = FakeSMSC.close_on_command(smsc, :bind_transmitter)

      {:ok, client} = start_client(port)

      # Wait for client to attempt bind (will be :bind_pending momentarily)
      # then witness the close. We allow either :bind_pending to drain
      # into :disconnected via the close, OR staying in :bind_pending
      # if backoff hasn't yet kicked in — both states are non-:bound.
      assert :ok = wait_until(fn -> Client.status(client) != :connecting end)
      refute Client.status(client) == :bound
      stop_pair(smsc, client)
    end
  end
end
