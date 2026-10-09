defmodule JasminEx.HttpApi.ConfigTest do
  use ExUnit.Case, async: true

  alias JasminEx.HttpApi.Config

  test "defaults multipart settings to UDH and five segments" do
    assert %Config{
             enabled: false,
             host: {127, 0, 0, 1},
             port: 1401,
             concat: :udh,
             max_segments: 5
           } = Config.new()
  end

  test "accepts server-owned SAR and bounded segment settings" do
    assert %Config{enabled: true, concat: :sar, max_segments: 1} =
             Config.new(enabled: true, concat: :sar, max_segments: 1)

    assert %Config{concat: :udh, max_segments: 5} =
             Config.new(concat: :udh, max_segments: 5)
  end

  test "rejects malformed multipart settings without atom conversion" do
    unknown_concat = "unknown-http-multipart-concat"

    assert_raise ArgumentError, "HTTP API concat must be :udh or :sar", fn ->
      Config.new(concat: unknown_concat)
    end

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_concat) end

    for value <- [0, 6, 1.5, "5", nil] do
      assert_raise ArgumentError, "HTTP API max_segments must be an integer from 1 to 5", fn ->
        Config.new(max_segments: value)
      end
    end
  end

  test "rejects non-keyword configuration before starting the listener" do
    assert_raise ArgumentError, "HTTP API configuration must be a keyword list", fn ->
      Config.new(%{})
    end
  end
end
