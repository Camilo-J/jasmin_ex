defmodule JasminEx.Smpp.Client.ReconnectPolicyTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.Client.ReconnectPolicy

  test "normalizes defaults with first-keyword-wins semantics" do
    assert %ReconnectPolicy{base_ms: 1_000, factor: 2, cap_ms: 30_000, jitter: true} =
             ReconnectPolicy.new([])

    assert %ReconnectPolicy{base_ms: 10, factor: 3, cap_ms: 90, jitter: false} =
             ReconnectPolicy.new(
               reconnect_base_ms: 10,
               reconnect_base_ms: 20,
               reconnect_factor: 3,
               reconnect_factor: 4,
               reconnect_cap_ms: 90,
               reconnect_cap_ms: 100,
               reconnect_jitter: false,
               reconnect_jitter: true
             )
  end

  test "calculates zero-based exponential attempts and caps before returning" do
    policy = %ReconnectPolicy{base_ms: 100, factor: 2, cap_ms: 500, jitter: false}
    random = fn _upper_bound -> flunk("random source called with jitter disabled") end

    assert ReconnectPolicy.delay(policy, 0, random) == 100
    assert ReconnectPolicy.delay(policy, 1, random) == 200
    assert ReconnectPolicy.delay(policy, 2, random) == 400
    assert ReconnectPolicy.delay(policy, 3, random) == 500
  end

  test "returns deterministic injected jitter and passes it the capped delay" do
    policy = %ReconnectPolicy{base_ms: 100, factor: 3, cap_ms: 250, jitter: true}

    random = fn upper_bound ->
      send(self(), {:upper_bound, upper_bound})
      div(upper_bound, 2)
    end

    assert ReconnectPolicy.delay(policy, 2, random) == 125
    assert_received {:upper_bound, 250}
  end
end
