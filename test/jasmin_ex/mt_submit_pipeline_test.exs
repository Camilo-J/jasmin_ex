defmodule JasminEx.MtSubmitPipelineTest do
  use ExUnit.Case, async: true

  alias JasminEx.MtSubmitPipeline

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

  defp spy(order, name, transform \\ & &1) do
    fn msg ->
      Agent.update(order, &(&1 ++ [name]))
      {:ok, transform.(msg)}
    end
  end
end
