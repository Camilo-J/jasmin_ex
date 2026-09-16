defmodule JasminEx.Dlr.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.RetryPolicy

  test "lookup allows 2 additional attempts and 3 total processing attempts" do
    assert RetryPolicy.additional_attempts(:lookup) == 2
    assert RetryPolicy.total_attempts(:lookup) == 3
    assert RetryPolicy.delay_ms(:lookup) == 10_000
  end

  test "HTTP allows 3 additional attempts and 4 total processing attempts" do
    assert RetryPolicy.additional_attempts(:http) == 3
    assert RetryPolicy.total_attempts(:http) == 4
    assert RetryPolicy.delay_ms(:http) == 30_000
  end

  test "missing count on a fresh delivery is zero failures" do
    meta = %{redelivered: false, headers: :undefined}
    assert RetryPolicy.failures(:lookup, meta) == {:ok, 0}
    assert RetryPolicy.settle(:lookup, meta, :retry) == {:reject, requeue: true}
  end

  test "basic.reject requeue is the counted retry path" do
    meta = headers(false, [{"x-delivery-count", :long, 0}])
    assert RetryPolicy.settle(:lookup, meta, :retry) == {:reject, requeue: true}

    last = headers(true, [{"x-delivery-count", :long, 2}])
    assert RetryPolicy.settle(:lookup, last, :retry) == {:reject, requeue: false}
  end

  test "nack is not a counted retry settlement" do
    meta = headers(false, [])
    assert {:reject, requeue: true} = RetryPolicy.settle(:lookup, meta, :retry)
    assert {:reject, requeue: true} = RetryPolicy.settle(:http, meta, :retry)
    assert RetryPolicy.settle(:lookup, meta, :ok) == :ack
    assert RetryPolicy.settle(:lookup, meta, :terminal) == {:reject, requeue: false}
  end

  test "missing or malformed counters on redelivery fail closed" do
    assert RetryPolicy.failures(:lookup, %{redelivered: true, headers: :undefined}) ==
             {:error, :malformed_counters}

    assert RetryPolicy.failures(:http, headers(true, [])) == {:error, :malformed_counters}

    assert RetryPolicy.failures(:lookup, headers(true, [{"x-delivery-count", :longstr, "1"}])) ==
             {:error, :malformed_counters}

    assert RetryPolicy.settle(:lookup, %{redelivered: true, headers: :undefined}, :retry) ==
             {:reject, requeue: false}
  end

  test "forged JSON producer counters are ignored; only typed AMQP headers count" do
    meta = %{
      redelivered: false,
      headers: [{"x-delivery-count", :long, 0}],
      payload: ~s({"x-delivery-count": 99, "attempts": 99})
    }

    assert RetryPolicy.failures(:lookup, meta) == {:ok, 0}

    assert RetryPolicy.failures(:lookup, Map.put(meta, :payload, %{"x-delivery-count" => 99})) ==
             {:ok, 0}
  end

  test "acquired-count is a conservative extra guard" do
    nack_loop =
      headers(true, [
        {"x-delivery-count", :long, 0},
        {"x-acquired-count", :long, 2}
      ])

    assert RetryPolicy.failures(:lookup, nack_loop) == {:ok, 0}

    malformed_acquired =
      headers(true, [
        {"x-delivery-count", :long, 1},
        {"x-acquired-count", :longstr, "nope"}
      ])

    assert RetryPolicy.failures(:lookup, malformed_acquired) == {:error, :malformed_counters}

    runaway =
      headers(true, [
        {"x-delivery-count", :long, 0},
        {"x-acquired-count", :long, 100}
      ])

    assert RetryPolicy.failures(:lookup, runaway) == {:error, :exhausted}
  end

  test "HTTP budget is exhausted after 3 additional failures" do
    assert RetryPolicy.settle(:http, headers(true, [{"x-delivery-count", :long, 2}]), :retry) ==
             {:reject, requeue: true}

    assert RetryPolicy.settle(:http, headers(true, [{"x-delivery-count", :long, 3}]), :retry) ==
             {:reject, requeue: false}
  end

  defp headers(redelivered, headers) do
    %{redelivered: redelivered, headers: headers}
  end
end
