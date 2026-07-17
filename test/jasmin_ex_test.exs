defmodule JasminExTest do
  use ExUnit.Case
  doctest JasminEx

  test "greets the world" do
    assert JasminEx.hello() == :world
  end
end
