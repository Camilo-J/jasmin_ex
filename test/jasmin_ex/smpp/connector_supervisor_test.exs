defmodule JasminEx.Smpp.ConnectorSupervisorTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias JasminEx.Application
  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.ConnectorSupervisor.Instance
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU.Body

  @invalid_connectors_message "invalid SMPP connector configuration: expected [], one non-empty keyword connector configuration, or a list of non-empty keyword connector configurations"

  test "gives each connector an explicit three-restarts-in-five-seconds intensity" do
    assert {:ok, {flags, [child]}} = Instance.init(connector_config(1111))
    assert %{intensity: 3, period: 5, strategy: :one_for_one} = flags
    assert child.restart == :transient
    assert child.start == {Client, :start_link, [connector_config(1111)]}
  end

  test "normalizes zero, one, and multiple connector configurations" do
    first = connector_config(1111)
    second = connector_config(2222)

    assert {:ok, {_flags, []}} = ConnectorSupervisor.init([])

    assert {:ok, {_flags, [single]}} = ConnectorSupervisor.init(first)
    assert single.id == {:smpp_connector, first[:connector_id]}
    assert single.start == {Instance, :start_link, [first]}
    assert single.restart == :transient

    assert {:ok, {_flags, [first_child, second_child]}} =
             ConnectorSupervisor.init([first, second])

    assert first_child.id == {:smpp_connector, first[:connector_id]}
    assert second_child.id == {:smpp_connector, second[:connector_id]}
    refute first_child.id == second_child.id
    assert first_child.start == {Instance, :start_link, [first]}
    assert second_child.start == {Instance, :start_link, [second]}
  end

  test "rejects a missing, blank, or non-binary connector_id" do
    valid = connector_config(1111)

    assert_raise KeyError, ~r/key :connector_id not found/, fn ->
      ConnectorSupervisor.init(Keyword.delete(valid, :connector_id))
    end

    message = fn value ->
      ":connector_id must be a non-empty binary, got: #{inspect(value)}"
    end

    for value <- ["", :alpha] do
      assert_raise ArgumentError, message.(value), fn ->
        ConnectorSupervisor.init(Keyword.put(valid, :connector_id, value))
      end
    end
  end

  test "rejects invalid top-level and malformed list shapes" do
    valid = connector_config(1111)

    for invalid <- [%{}, :invalid, [valid, :invalid], [valid, []], [[:host]]] do
      assert_raise ArgumentError, @invalid_connectors_message, fn ->
        ConnectorSupervisor.init(invalid)
      end
    end
  end

  test "restarts a transient client with a new PID after abnormal exit" do
    {:ok, port, smsc} = FakeSMSC.start_link()
    {:ok, supervisor} = ConnectorSupervisor.start_link(connector_config(port))

    connector = child_pid(supervisor, {:smpp_connector, "connector-#{port}"})
    first_client = child_pid(connector, :smpp_client)
    assert :ok = wait_until(fn -> Client.status(first_client) == :bound end)

    Process.exit(first_client, :kill)

    assert :ok =
             wait_until(fn ->
               case Supervisor.which_children(connector) do
                 [{_id, client, :worker, [Client]}] when is_pid(client) ->
                   client != first_client and Client.status(client) == :bound

                 _children ->
                   false
               end
             end)

    restarted_client = child_pid(connector, :smpp_client)
    refute restarted_client == first_client

    GenServer.stop(supervisor)
    GenServer.stop(smsc)
  end

  test "supervises a transient client that is not restarted after graceful unbind" do
    {:ok, port, smsc} = FakeSMSC.start_link()

    {:ok, supervisor} =
      ConnectorSupervisor.start_link(connector_config(port))

    connector = child_pid(supervisor, {:smpp_connector, "connector-#{port}"})
    client = child_pid(connector, :smpp_client)
    assert :ok = wait_until(fn -> Client.status(client) == :bound end)
    monitor = Process.monitor(client)
    assert :ok = Client.unbind(client)
    assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 500

    assert :ok =
             wait_until(fn ->
               Enum.all?(Supervisor.which_children(connector), fn {_id, pid, _type, _modules} ->
                 pid == :undefined
               end)
             end)

    GenServer.stop(supervisor)
    GenServer.stop(smsc)
  end

  test "a connector crash loop does not terminate or disrupt a healthy sibling" do
    {:ok, crashing_port, crashing_smsc} = FakeSMSC.start_link()
    {:ok, healthy_port, healthy_smsc} = FakeSMSC.start_link()

    {:ok, supervisor} =
      ConnectorSupervisor.start_link([
        connector_config(crashing_port),
        connector_config(healthy_port)
      ])

    crashing_connector = child_pid(supervisor, {:smpp_connector, "connector-#{crashing_port}"})
    healthy_connector = child_pid(supervisor, {:smpp_connector, "connector-#{healthy_port}"})
    crashing_client = child_pid(crashing_connector, :smpp_client)
    healthy_client = child_pid(healthy_connector, :smpp_client)

    assert :ok = wait_until(fn -> Client.status(crashing_client) == :bound end)
    assert :ok = wait_until(fn -> Client.status(healthy_client) == :bound end)

    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:jasmin_ex, :smpp, :bound],
        &__MODULE__.handle_bound/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    crashing_client =
      Enum.reduce(1..3, crashing_client, fn _restart, client ->
        client_ref = Process.monitor(client)
        Process.exit(client, :kill)
        assert_receive {:DOWN, ^client_ref, :process, ^client, :killed}, 500
        assert_receive {:connector_bound, restarted_client}, 500
        assert restarted_client == child_pid(crashing_connector, :smpp_client)
        restarted_client
      end)

    connector_ref = Process.monitor(crashing_connector)
    client_ref = Process.monitor(crashing_client)
    Process.exit(crashing_client, :kill)

    assert_receive {:DOWN, ^client_ref, :process, ^crashing_client, :killed}, 500
    assert_receive {:DOWN, ^connector_ref, :process, ^crashing_connector, :shutdown}, 500

    assert Process.alive?(supervisor)

    assert healthy_connector ==
             child_pid(supervisor, {:smpp_connector, "connector-#{healthy_port}"})

    assert healthy_client == child_pid(healthy_connector, :smpp_client)
    assert Client.status(healthy_client) == :bound

    assert {:ok, "fake-msg-id"} =
             Client.send_submit_sm(healthy_client, submit_sm("still-healthy"))

    GenServer.stop(supervisor)
    GenServer.stop(crashing_smsc)
    GenServer.stop(healthy_smsc)
  end

  def handle_bound(_event, _measurements, metadata, test_pid) do
    send(test_pid, {:connector_bound, metadata.client})
  end

  def handle_lifecycle(_event, _measurements, metadata, test_pid) do
    send(test_pid, {:lifecycle, metadata})
  end

  test "bound and disconnected telemetry include the stable connector_id" do
    {:ok, port, smsc} = FakeSMSC.start_link()
    connector_id = "connector-#{port}"
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [[:jasmin_ex, :smpp, :bound], [:jasmin_ex, :smpp, :disconnected]],
        &__MODULE__.handle_lifecycle/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, supervisor} = ConnectorSupervisor.start_link(connector_config(port))
    connector = child_pid(supervisor, {:smpp_connector, connector_id})
    client = child_pid(connector, :smpp_client)
    assert :ok = wait_until(fn -> Client.status(client) == :bound end)

    assert_receive {:lifecycle, %{client: ^client, connector_id: ^connector_id, kind: :initial}},
                   500

    GenServer.stop(smsc)

    assert_receive {:lifecycle,
                    %{
                      client: ^client,
                      connector_id: ^connector_id,
                      state: :bound
                    }},
                   500

    GenServer.stop(supervisor)
  end

  test "application adds a connector supervisor only when connector config is present" do
    assert [%{id: JasminEx.StateStore.Connection}] = Application.children([])

    assert [%{id: JasminEx.StateStore.Connection}, {ConnectorSupervisor, [[]]}] =
             Application.children(smpp_connectors: [[]])
  end

  defp connector_config(port) do
    [
      connector_id: "connector-#{port}",
      host: ~c"localhost",
      port: port,
      system_id: "user",
      password: "pw",
      system_type: "type",
      bind_as: :transmitter,
      heartbeat_ms: 10_000,
      response_timeout_ms: 100,
      reconnect_base_ms: 5,
      reconnect_cap_ms: 5,
      reconnect_jitter: false
    ]
  end

  defp child_pid(supervisor, id) do
    {^id, pid, _type, _modules} =
      Enum.find(Supervisor.which_children(supervisor), fn {child_id, _, _, _} ->
        child_id == id
      end)

    pid
  end

  defp submit_sm(message) do
    %Body.SubmitSM{
      source_addr_ton: :INTERNATIONAL,
      source_addr_npi: :ISDN,
      source_addr: "src",
      dest_addr_ton: :NATIONAL,
      dest_addr_npi: :ISDN,
      destination_addr: "5551000",
      short_message: message
    }
  end

  defp wait_until(predicate, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_until_deadline(predicate, deadline)
  end

  defp wait_until_deadline(predicate, deadline) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(5)
        wait_until_deadline(predicate, deadline)
    end
  end
end
