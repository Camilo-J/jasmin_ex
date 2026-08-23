Code.require_file(Path.expand("../../support/fake_clock.ex", __DIR__))

defmodule JasminEx.Billing.BillTest do
  use ExUnit.Case, async: true

  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Clock
  alias JasminEx.Billing.FakeClock
  alias JasminEx.Billing.Fingerprint

  @max_int64 9_223_372_036_854_775_807

  describe "money and rate values" do
    test "rejects negative amounts unchanged" do
      assert Bill.new(valid_attrs(rate_minor: -1)) == {:error, :invalid_amount}
      assert Bill.new(valid_attrs(rate_minor: -100)) == {:error, :invalid_amount}
    end

    test "rejects overflow amounts unchanged" do
      assert Bill.new(valid_attrs(rate_minor: @max_int64 + 1)) == {:error, :amount_overflow}
      assert Bill.new(valid_attrs(rate_minor: @max_int64 + 99)) == {:error, :amount_overflow}
    end

    test "rejects a pre-charge percentage outside 0-100 unchanged" do
      assert Bill.new(valid_attrs(precharge_percent: -1)) == {:error, :invalid_percentage}
      assert Bill.new(valid_attrs(precharge_percent: 101)) == {:error, :invalid_percentage}
    end

    test "splits an exact integer pre-charge including a non-divisible rate" do
      assert {:ok, bill} = Bill.new(valid_attrs(rate_minor: 100, precharge_percent: 10))
      assert bill.precharge_minor == 10
      assert bill.remainder_minor == 90
      assert bill.quota_debit == 1

      assert {:ok, uneven} = Bill.new(valid_attrs(rate_minor: 10, precharge_percent: 33))
      assert uneven.precharge_minor == 3
      assert uneven.remainder_minor == 7
    end

    test "captures nothing at 0% and everything at 100%" do
      assert {:ok, zero} = Bill.new(valid_attrs(rate_minor: 250, precharge_percent: 0))
      assert zero.precharge_minor == 0
      assert zero.remainder_minor == 250

      assert {:ok, full} = Bill.new(valid_attrs(rate_minor: 250, precharge_percent: 100))
      assert full.precharge_minor == 250
      assert full.remainder_minor == 0
    end

    test "rejects invalid bill id, uid, and route order" do
      assert Bill.new(valid_attrs(bill_id: "")) == {:error, :invalid_bill_id}
      assert Bill.new(valid_attrs(bill_id: :binary.copy("b", 129))) == {:error, :invalid_bill_id}
      assert Bill.new(valid_attrs(uid: "")) == {:error, :invalid_uid}
      assert Bill.new(valid_attrs(uid: :binary.copy("u", 129))) == {:error, :invalid_uid}
      assert Bill.new(valid_attrs(route_order: -1)) == {:error, :invalid_route}
      assert Bill.new("not-a-keyword-list") == {:error, :invalid_bill_id}
    end

    test "accepts boundary identifiers and the signed 64-bit rate maximum" do
      assert {:ok, bill} =
               Bill.new(
                 valid_attrs(
                   bill_id: :binary.copy("b", 128),
                   uid: :binary.copy("u", 128),
                   route_order: 0,
                   rate_minor: @max_int64,
                   precharge_percent: 100
                 )
               )

      assert bill.precharge_minor == @max_int64
      assert bill.remainder_minor == 0
      assert bill.quota_debit == 1
    end

    test "redacts economic payloads from Inspect" do
      assert {:ok, bill} = Bill.new(valid_attrs(rate_minor: 987_654, precharge_percent: 25))
      inspected = inspect(bill)

      refute inspected =~ "987654"
      refute inspected =~ Integer.to_string(bill.precharge_minor)
      refute inspected =~ Integer.to_string(bill.remainder_minor)
      assert inspected =~ "REDACTED"
    end
  end

  describe "fingerprint" do
    test "hashes length-prefixed economics and excludes bill_id" do
      assert {:ok, left} = Bill.new(valid_attrs(bill_id: "bill-a"))
      assert {:ok, right} = Bill.new(valid_attrs(bill_id: "bill-b"))
      assert {:ok, %Fingerprint{version: 1, digest: digest}} = Fingerprint.compute(left)
      assert {:ok, %Fingerprint{digest: ^digest}} = Fingerprint.compute(right)
      assert digest == expected_digest(left)
      assert byte_size(digest) == 32
    end

    test "changes when any economic field changes" do
      assert {:ok, base} = Bill.new(valid_attrs())
      assert {:ok, other_uid} = Bill.new(valid_attrs(uid: "user-2"))
      assert {:ok, other_rate} = Bill.new(valid_attrs(rate_minor: 50))
      assert {:ok, base_fp} = Fingerprint.compute(base)
      assert {:ok, uid_fp} = Fingerprint.compute(other_uid)
      assert {:ok, rate_fp} = Fingerprint.compute(other_rate)
      refute base_fp.digest == uid_fp.digest
      refute base_fp.digest == rate_fp.digest
    end

    test "redacts the digest from Inspect" do
      assert {:ok, bill} = Bill.new(valid_attrs())
      assert {:ok, fingerprint} = Fingerprint.compute(bill)
      inspected = inspect(fingerprint)

      refute inspected =~ Base.encode16(fingerprint.digest, case: :lower)
      refute inspected =~ inspect(fingerprint.digest)
      assert inspected =~ "REDACTED"
    end
  end

  describe "clock" do
    test "system clock returns millisecond integers for wall and monotonic time" do
      wall = Clock.System.wall_ms()
      monotonic = Clock.System.monotonic_ms()

      assert is_integer(wall)
      assert is_integer(monotonic)
      assert Clock.wall_ms(Clock.System) >= wall
      assert Clock.monotonic_ms(Clock.System) >= monotonic
    end

    test "fake clock is deterministic and isolated per value" do
      first = FakeClock.new(wall_ms: 1_000, monotonic_ms: 10)
      second = FakeClock.new(wall_ms: 9_000, monotonic_ms: 90)

      assert FakeClock.wall_ms(first) == 1_000
      assert FakeClock.monotonic_ms(first) == 10
      assert Clock.wall_ms({FakeClock, first}) == 1_000
      assert Clock.monotonic_ms({FakeClock, second}) == 90

      advanced = FakeClock.advance(first, 250)
      assert FakeClock.wall_ms(advanced) == 1_250
      assert FakeClock.monotonic_ms(advanced) == 260
      assert FakeClock.wall_ms(first) == 1_000
      assert FakeClock.wall_ms(second) == 9_000
    end
  end

  defp valid_attrs(overrides \\ []) do
    [
      bill_id: "bill-1",
      uid: "user-1",
      route_order: 0,
      rate_minor: 100,
      precharge_percent: 10
    ]
    |> Keyword.merge(overrides)
  end

  defp expected_digest(%Bill{} = bill) do
    payload =
      encode_field(bill.uid) <>
        encode_field(bill.rate_minor) <>
        encode_field(bill.precharge_percent) <>
        encode_field(bill.precharge_minor) <>
        encode_field(bill.remainder_minor) <>
        encode_field(bill.quota_debit)

    :crypto.hash(:sha256, payload)
  end

  defp encode_field(value) when is_binary(value), do: <<byte_size(value)::32-big, value::binary>>

  defp encode_field(value) when is_integer(value) do
    encoded = Integer.to_string(value)
    <<byte_size(encoded)::32-big, encoded::binary>>
  end
end
