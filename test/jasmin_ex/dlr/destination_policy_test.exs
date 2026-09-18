defmodule JasminEx.Dlr.DestinationPolicyTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.DestinationPolicy

  defmodule Resolver do
    def resolve(table, host), do: Map.fetch!(table, host)
  end

  test "accepts only HTTP(S) origins without authority ambiguity" do
    resolver = resolver(%{"public.example" => {:ok, [{93, 184, 216, 34}]}})

    assert {:ok, approved} =
             DestinationPolicy.approve("https://public.example:8443/dlr", resolver: resolver)

    assert approved.original_host == "public.example"
    assert approved.peer == {93, 184, 216, 34}
    assert approved.port == 8443
    assert approved.host_header == "public.example:8443"
    assert approved.request_url == "https://93.184.216.34:8443/dlr"

    for url <- [
          "ftp://public.example/dlr",
          "https://user:pass@public.example/dlr",
          "https://public.example/dlr#fragment",
          "https://public.example:0/dlr",
          "https://public.example/dlr\nnext"
        ] do
      assert {:error, _reason} = DestinationPolicy.approve(url, resolver: resolver)
    end
  end

  test "rejects reserved callback-field query collisions" do
    resolver = resolver(%{"public.example" => {:ok, [{93, 184, 216, 34}]}})

    assert {:error, :reserved_query_field} =
             DestinationPolicy.approve("https://public.example/dlr?id=attacker",
               resolver: resolver
             )

    assert {:ok, _approved} =
             DestinationPolicy.approve("https://public.example/dlr?tenant=one",
               resolver: resolver
             )
  end

  test "rejects private, loopback, link-local, multicast, unspecified, and mapped IPv6" do
    forbidden = [
      {127, 0, 0, 1},
      {10, 0, 0, 1},
      {172, 16, 0, 1},
      {192, 168, 0, 1},
      {169, 254, 169, 254},
      {0, 0, 0, 0},
      {224, 0, 0, 1},
      {0, 0, 0, 0, 0, 0, 0, 1},
      {0xFC00, 0, 0, 0, 0, 0, 0, 1},
      {0xFE80, 0, 0, 0, 0, 0, 0, 1},
      {0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1}
    ]

    for address <- forbidden do
      resolver = resolver(%{"callback.test" => {:ok, [address]}})

      assert {:error, :forbidden_address} =
               DestinationPolicy.approve("https://callback.test/dlr", resolver: resolver)
    end
  end

  test "rejects an answer set containing a forbidden rebinding address" do
    resolver =
      resolver(%{
        "callback.test" => {:ok, [{93, 184, 216, 34}, {127, 0, 0, 1}]}
      })

    assert {:error, :forbidden_address} =
             DestinationPolicy.approve("https://callback.test/dlr", resolver: resolver)
  end

  test "explicit exact host/address exception is narrow and returns a pinned peer" do
    resolver = resolver(%{"callback.test" => {:ok, [{127, 0, 0, 1}]}})

    assert {:error, :forbidden_address} =
             DestinationPolicy.approve("http://callback.test/dlr", resolver: resolver)

    assert {:ok, approved} =
             DestinationPolicy.approve("http://callback.test/dlr",
               resolver: resolver,
               allow: [{"callback.test", {127, 0, 0, 1}}]
             )

    assert approved.peer == {127, 0, 0, 1}
    assert approved.original_host == "callback.test"

    assert {:error, :forbidden_address} =
             DestinationPolicy.approve("http://other.test/dlr",
               resolver: resolver(%{"other.test" => {:ok, [{127, 0, 0, 1}]}}),
               allow: [{"callback.test", {127, 0, 0, 1}}]
             )
  end

  defp resolver(table), do: {Resolver, table}
end
