defmodule JasminEx.MtSubmitPipelineTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.Envelope
  alias JasminEx.MtSubmitPipeline
  alias JasminEx.MtSubmitPipeline.Production
  alias JasminEx.Routing
  alias JasminEx.Routing.Config
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Filter
  alias JasminEx.Routing.Router

  test "runs validate, intercept, route, qos, bill, then billed dispatch" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: spy(order, :validate),
      intercept: spy(order, :intercept),
      route: spy(order, :route),
      qos: spy(order, :qos),
      bill: spy(order, :bill, fn msg -> Map.put(msg, :billed, true) end),
      dispatch: spy(order, :dispatch, fn msg -> {:dispatched, msg} end)
    }

    input = %{uid: "u1", destination: "123", content: "hello"}
    assert {:ok, {:dispatched, result}} = MtSubmitPipeline.run(input, stages)
    assert Agent.get(order, & &1) == [:validate, :intercept, :route, :qos, :bill, :dispatch]
    assert result.billed
    assert result.content == "hello"
  end

  test "omitted interceptor and qos stay no-ops and still bill then dispatch" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: spy(order, :validate),
      route: spy(order, :route, fn msg -> Map.put(msg, :connector, "c1") end),
      bill: spy(order, :bill, fn msg -> Map.put(msg, :billed, true) end),
      dispatch: spy(order, :dispatch, fn msg -> {:dispatched, msg} end)
    }

    input = %{uid: "u2", destination: "555", content: "ping"}
    assert {:ok, {:dispatched, result}} = MtSubmitPipeline.run(input, stages)
    assert Agent.get(order, & &1) == [:validate, :route, :bill, :dispatch]

    assert result == %{
             uid: "u2",
             destination: "555",
             content: "ping",
             connector: "c1",
             billed: true
           }
  end

  test "invalid input fails at validate and does not route, bill, or dispatch" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: fn _input ->
        Agent.update(order, &(&1 ++ [:validate]))
        {:error, :invalid}
      end,
      intercept: spy(order, :intercept),
      route: spy(order, :route),
      qos: spy(order, :qos),
      bill: spy(order, :bill),
      dispatch: spy(order, :dispatch)
    }

    assert {:error, {:validate, :invalid}} = MtSubmitPipeline.run(%{content: ""}, stages)
    assert Agent.get(order, & &1) == [:validate]
  end

  test "a different validate error still skips route, bill, and dispatch" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: fn _input ->
        Agent.update(order, &(&1 ++ [:validate]))
        {:error, :missing_destination}
      end,
      route: spy(order, :route),
      bill: spy(order, :bill),
      dispatch: spy(order, :dispatch)
    }

    assert {:error, {:validate, :missing_destination}} =
             MtSubmitPipeline.run(%{uid: "u3"}, stages)

    assert Agent.get(order, & &1) == [:validate]
  end

  test "missing validate returns typed error without later stages" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      route: spy(order, :route),
      bill: spy(order, :bill),
      dispatch: spy(order, :dispatch)
    }

    assert {:error, {:validate, :missing_port}} =
             MtSubmitPipeline.run(%{uid: "u4", destination: "1", content: "x"}, stages)

    assert Agent.get(order, & &1) == []
  end

  test "missing route returns typed error without later stages" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: spy(order, :validate),
      bill: spy(order, :bill),
      dispatch: spy(order, :dispatch)
    }

    assert {:error, {:route, :missing_port}} =
             MtSubmitPipeline.run(%{uid: "u5", destination: "2", content: "y"}, stages)

    assert Agent.get(order, & &1) == [:validate]
  end

  test "missing bill returns typed error without later stages" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: spy(order, :validate),
      route: spy(order, :route),
      dispatch: spy(order, :dispatch)
    }

    assert {:error, {:bill, :missing_port}} =
             MtSubmitPipeline.run(%{uid: "u6", destination: "3", content: "z"}, stages)

    assert Agent.get(order, & &1) == [:validate, :route]
  end

  test "missing dispatch returns typed error without later stages" do
    {:ok, order} = Agent.start_link(fn -> [] end)

    stages = %{
      validate: spy(order, :validate),
      route: spy(order, :route),
      bill: spy(order, :bill)
    }

    assert {:error, {:dispatch, :missing_port}} =
             MtSubmitPipeline.run(%{uid: "u7", destination: "4", content: "w"}, stages)

    assert Agent.get(order, & &1) == [:validate, :route, :bill]
  end

  defmodule FakeQueue do
    def enqueue(agent, envelope) do
      Agent.get_and_update(agent, fn state ->
        {state.reply, %{state | envelopes: state.envelopes ++ [envelope]}}
      end)
    end

    def start(reply) do
      {:ok, pid} = Agent.start_link(fn -> %{reply: reply, envelopes: []} end)
      pid
    end

    def envelopes(agent), do: Agent.get(agent, & &1.envelopes)
  end

  describe "production submit" do
    @describetag :tmp_dir

    test "HTTP submit reuses MtSubmitPipeline.run/2 with production adapters", %{tmp_dir: tmp_dir} do
      {router, queue} = start_pipeline(tmp_dir)
      opts = opts(router, queue, "mid-run")
      input = valid_input()

      assert {:ok, "mid-run"} = MtSubmitPipeline.run(input, Production.stages(opts))
      assert [%Envelope{gateway_id: "mid-run"}] = FakeQueue.envelopes(queue)

      {router2, queue2} = start_pipeline(tmp_dir, file: "routing-2.json")

      assert {:ok, "mid-submit"} =
               MtSubmitPipeline.submit(input, opts(router2, queue2, "mid-submit"))

      assert [%Envelope{gateway_id: "mid-submit"}] = FakeQueue.envelopes(queue2)
      refute_secret({:ok, "mid-submit"})
    end

    test "happy path returns one secret-free message id used by bill and envelope", %{
      tmp_dir: tmp_dir
    } do
      {router, queue} = start_pipeline(tmp_dir)

      assert {:ok, "mid-1"} = MtSubmitPipeline.submit(valid_input(), opts(router, queue, "mid-1"))

      [envelope] = FakeQueue.envelopes(queue)
      assert envelope.gateway_id == "mid-1"
      assert envelope.connector_id == "smpp-t"

      assert envelope.submit_sm == %{
               source_addr: "1616",
               destination_addr: "21200000",
               short_message: "hello",
               data_coding: 0
             }

      snapshot = Routing.snapshot(router)
      assert snapshot.reservations["mid-1"].state == :open
      refute_secret({:ok, "mid-1"})
    end

    test "hex content and coding 8 round-trip through the queued envelope", %{tmp_dir: tmp_dir} do
      {router, queue} = start_pipeline(tmp_dir)

      input =
        valid_input()
        |> Map.delete(:content)
        |> Map.merge(%{hex_content: Base.encode16("hola"), coding: 8})

      assert {:ok, "mid-hex"} = MtSubmitPipeline.submit(input, opts(router, queue, "mid-hex"))
      [envelope] = FakeQueue.envelopes(queue)
      assert envelope.submit_sm.short_message == "hola"
      assert envelope.submit_sm.data_coding == 8
    end

    test "validation failures are typed, skip later stages, and leak no secrets", %{
      tmp_dir: tmp_dir
    } do
      {router, queue} = start_pipeline(tmp_dir)
      opts = opts(router, queue, "mid-v")

      assert {:error, {:validate, :missing_to}} =
               MtSubmitPipeline.submit(Map.delete(valid_input(), :to), opts)

      assert {:error, {:validate, :missing_from}} =
               MtSubmitPipeline.submit(Map.delete(valid_input(), :from), opts)

      assert {:error, {:validate, :missing_content}} =
               MtSubmitPipeline.submit(Map.delete(valid_input(), :content), opts)

      assert {:error, {:validate, :ambiguous_content}} =
               MtSubmitPipeline.submit(Map.put(valid_input(), :hex_content, "00"), opts)

      assert {:error, {:validate, :malformed_hex}} =
               MtSubmitPipeline.submit(
                 valid_input() |> Map.delete(:content) |> Map.put(:hex_content, "zz"),
                 opts
               )

      assert {:error, {:validate, :invalid_coding}} =
               MtSubmitPipeline.submit(Map.put(valid_input(), :coding, 99), opts)

      secret = Map.merge(valid_input(), %{password: "s3cret", tags: ["x"]})
      assert {:error, {:validate, :unknown_field}} = MtSubmitPipeline.submit(secret, opts)
      refute_secret({:error, {:validate, :unknown_field}})
      assert FakeQueue.envelopes(queue) == []
      assert Routing.snapshot(router).reservations == %{}
    end

    test "route failure is typed and does not bill or dispatch", %{tmp_dir: tmp_dir} do
      {router, queue} = start_pipeline(tmp_dir)
      opts = opts(router, queue, "mid-r")
      miss = Map.put(valid_input(), :to, "999")

      assert {:error, {:route, :no_route}} = MtSubmitPipeline.submit(miss, opts)
      refute_secret({:error, {:route, :no_route}})
      assert FakeQueue.envelopes(queue) == []
      assert Routing.snapshot(router).reservations == %{}
    end

    test "billing failures are typed and do not dispatch", %{tmp_dir: tmp_dir} do
      {router, queue} = start_pipeline(tmp_dir, balance_minor: 50)
      opts = opts(router, queue, "mid-b")

      assert {:error, {:bill, :insufficient_balance}} =
               MtSubmitPipeline.submit(valid_input(), opts)

      {router_q, queue_q} = start_pipeline(tmp_dir, file: "routing-q.json", submit_quota: 0)

      assert {:error, {:bill, :insufficient_quota}} =
               MtSubmitPipeline.submit(valid_input(), opts(router_q, queue_q, "mid-q"))

      refute_secret({:error, {:bill, :insufficient_quota}})
      assert FakeQueue.envelopes(queue) == []
      assert FakeQueue.envelopes(queue_q) == []
    end

    test "definite enqueue failure settles non_ok and is typed without secrets", %{
      tmp_dir: tmp_dir
    } do
      {router, queue} = start_pipeline(tmp_dir, queue_reply: {:error, :non_ok})

      assert {:error, {:dispatch, :non_ok}} =
               MtSubmitPipeline.submit(valid_input(), opts(router, queue, "mid-nack"))

      snapshot = Routing.snapshot(router)
      assert snapshot.reservations == %{}
      assert snapshot.tombstones["mid-nack"].state == :settled_non_ok
      refute_secret({:error, {:dispatch, :non_ok}})
    end

    test "ambiguous enqueue leaves the billing reservation open", %{tmp_dir: tmp_dir} do
      {router, queue} = start_pipeline(tmp_dir, queue_reply: {:ambiguous, :timeout})

      assert {:error, {:dispatch, {:ambiguous, :timeout}}} =
               MtSubmitPipeline.submit(valid_input(), opts(router, queue, "mid-to"))

      snapshot = Routing.snapshot(router)
      assert snapshot.reservations["mid-to"].state == :open
      assert snapshot.tombstones == %{}

      {router2, queue2} =
        start_pipeline(tmp_dir,
          file: "routing-cc.json",
          queue_reply: {:ambiguous, :channel_closed}
        )

      assert {:error, {:dispatch, {:ambiguous, :channel_closed}}} =
               MtSubmitPipeline.submit(valid_input(), opts(router2, queue2, "mid-cc"))

      assert Routing.snapshot(router2).reservations["mid-cc"].state == :open
    end
  end

  defp start_pipeline(tmp_dir, opts \\ []) do
    config =
      Config.new(snapshot_path: Path.join(tmp_dir, Keyword.get(opts, :file, "routing.json")))

    router = start_supervised!({Router, name: nil, config: config}, id: make_ref())
    {:ok, group} = Routing.put_group(router, gid: "ops")

    {:ok, _user} =
      Routing.put_user(router,
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: Keyword.get(opts, :balance_minor, 500),
        submit_quota: Keyword.get(opts, :submit_quota, 3)
      )

    {:ok, connector} = ConnectorRef.new("smpp-t")
    {:ok, dest} = Filter.Destination.new(address: "21200000")

    {:ok, _route} =
      Routing.put_route(router,
        kind: :static,
        order: 10,
        connector: connector,
        filters: [dest],
        rate_minor: 100,
        precharge_percent: 10
      )

    {router, FakeQueue.start(Keyword.get(opts, :queue_reply, :ok))}
  end

  defp opts(router, queue, bill_id) do
    %{router: router, queue: {FakeQueue, queue}, id_fun: fn -> bill_id end}
  end

  defp valid_input do
    %{uid: "u1", to: "21200000", from: "1616", content: "hello", coding: 0}
  end

  defp refute_secret(result) do
    inspected = inspect(result, limit: :infinity, printable_limit: :infinity)
    refute inspected =~ "s3cret"
    refute inspected =~ "password"
  end

  defp spy(order, name, transform \\ & &1) do
    fn msg ->
      Agent.update(order, &(&1 ++ [name]))
      {:ok, transform.(msg)}
    end
  end
end
