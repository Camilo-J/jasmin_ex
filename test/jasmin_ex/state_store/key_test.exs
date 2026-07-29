defmodule JasminEx.StateStore.KeyTest do
  use ExUnit.Case, async: true

  alias JasminEx.StateStore.Key

  test "frames arbitrary binary namespaces and keys without boundary collisions" do
    assert Key.encode({<<1, 2>>, "v"}, <<0, 255>>) != Key.encode({<<1>>, <<2, "v">>}, <<0, 255>>)

    assert Key.encode({"prefix", "v1"}, <<0, 255>>) ==
             <<"JXSS", 1, 6::32, "prefix", 2::16, "v1", 2::64, 0, 255>>
  end

  test "rejects invalid or oversized namespace components and keys" do
    assert {:error, {:invalid_argument, :key}} = Key.encode({"prefix", "v"}, :key)
    assert {:error, {:invalid_argument, :key_namespace}} = Key.encode({:prefix, "v"}, "key")

    assert {:error, {:invalid_argument, :key}} =
             Key.encode({"prefix", "v"}, :binary.copy("x", 65_536))
  end
end
