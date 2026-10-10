defmodule JasminEx.MtSubmitPipeline.ConcatReferenceTest do
  use ExUnit.Case, async: true

  alias JasminEx.MtSubmitPipeline.ConcatReference

  test "allocates one shared sequence from 1 through 255 then wraps" do
    {:ok, allocator} = ConcatReference.start_link(name: nil)

    assert Enum.map(1..255, fn _ -> ConcatReference.next(allocator) end) ==
             Enum.map(1..255, &{:ok, &1})

    assert {:ok, 1} = ConcatReference.next(allocator)
  end

  test "serializes concurrent allocation without promising uniqueness after wrap" do
    {:ok, allocator} = ConcatReference.start_link(name: nil)

    references =
      1..255
      |> Task.async_stream(fn _ -> ConcatReference.next(allocator) end, max_concurrency: 32)
      |> Enum.map(fn {:ok, {:ok, reference}} -> reference end)

    assert Enum.sort(references) == Enum.to_list(1..255)
    assert {:ok, 1} = ConcatReference.next(allocator)
  end

  test "restart resets the process-local sequence" do
    {:ok, allocator} = ConcatReference.start_link(name: nil)
    assert {:ok, 1} = ConcatReference.next(allocator)
    assert :ok = GenServer.stop(allocator)

    {:ok, restarted} = ConcatReference.start_link(name: nil)
    assert {:ok, 1} = ConcatReference.next(restarted)
  end
end
