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

  test "rejects non-global and special-purpose callback destinations" do
    forbidden = [
      {"IPv4 loopback", {127, 0, 0, 1}},
      {"IPv4 private 10/8", {10, 0, 0, 1}},
      {"IPv4 private 172.16/12", {172, 16, 0, 1}},
      {"IPv4 private 192.168/16", {192, 168, 0, 1}},
      {"IPv4 link-local metadata", {169, 254, 169, 254}},
      {"IPv4 unspecified", {0, 0, 0, 0}},
      {"IPv4 multicast", {224, 0, 0, 1}},
      {"IPv4-compatible/translatable", {0x64, 0xFF9B, 0, 0, 0, 0, 0xC000, 0x201}},
      {"discard-only", {0x100, 0, 0, 0, 0, 0, 0, 1}},
      {"Teredo", {0x2001, 0, 0, 0, 0, 0, 0, 1}},
      {"benchmarking", {0x2001, 2, 0, 0, 0, 0, 0, 1}},
      {"deprecated ORCHID", {0x2001, 0x10, 0, 0, 0, 0, 0, 1}},
      {"ORCHIDv2", {0x2001, 0x20, 0, 0, 0, 0, 0, 1}},
      {"documentation", {0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}},
      {"6to4 transition", {0x2002, 0xC000, 0x204, 0, 0, 0, 0, 1}},
      {"unique-local", {0xFC00, 0, 0, 0, 0, 0, 0, 1}},
      {"deprecated site-local", {0xFEC0, 0, 0, 0, 0, 0, 0, 1}},
      {"link-local", {0xFE80, 0, 0, 0, 0, 0, 0, 1}},
      {"multicast", {0xFF02, 0, 0, 0, 0, 0, 0, 1}},
      {"unspecified", {0, 0, 0, 0, 0, 0, 0, 0}},
      {"loopback", {0, 0, 0, 0, 0, 0, 0, 1}},
      {"IPv4-mapped private", {0, 0, 0, 0, 0, 0xFFFF, 0xA00, 1}}
    ]

    for {category, address} <- forbidden do
      resolver = resolver(%{"callback.test" => {:ok, [address]}})

      assert {:error, :forbidden_address} =
               DestinationPolicy.approve("https://callback.test/dlr", resolver: resolver),
             category
    end
  end

  test "accepts an ordinary global-unicast IPv6 destination" do
    address = {0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111}
    resolver = resolver(%{"callback.test" => {:ok, [address]}})

    assert {:ok, approved} =
             DestinationPolicy.approve("https://callback.test/dlr", resolver: resolver)

    assert approved.peer == address
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
