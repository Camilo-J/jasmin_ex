defmodule JasminEx.Routing.CredentialTest do
  use ExUnit.Case, async: true

  alias JasminEx.Routing.Credential

  test "hashes a secret with PBKDF2 600k iterations, 16-byte salt, and 32-byte digest" do
    assert {:ok, %Credential{} = credential} = Credential.hash("s3cret")

    assert credential.algorithm == "pbkdf2-hmac-sha256-v1"
    assert credential.iterations == 600_000
    assert byte_size(credential.salt) == 16
    assert byte_size(credential.digest) == 32

    assert credential.digest ==
             :crypto.pbkdf2_hmac(:sha256, "s3cret", credential.salt, 600_000, 32)
  end

  test "uses a unique salt so the same secret does not produce the same digest" do
    assert {:ok, first} = Credential.hash("s3cret")
    assert {:ok, second} = Credential.hash("s3cret")

    assert first.salt != second.salt
    assert first.digest != second.digest
    assert Credential.verify(first, "s3cret")
    refute Credential.verify(second, "other-secret")
  end

  test "compares derived digests with constant-time equality" do
    assert {:ok, credential} = Credential.hash("s3cret")
    assert Credential.verify(credential, "s3cret")
    refute Credential.verify(credential, "wrong-secret")
    refute Credential.verify(credential, "")

    source = File.read!("lib/jasmin_ex/routing/credential.ex")
    assert source =~ ":crypto.hash_equals("
  end

  test "redacts salt, digest, and the input secret from Inspect" do
    assert {:ok, credential} = Credential.hash("s3cret")
    inspected = inspect(credential)

    refute inspected =~ "s3cret"
    refute inspected =~ inspect(credential.salt)
    refute inspected =~ inspect(credential.digest)
    assert inspected =~ "REDACTED"
  end

  test "rejects a blank or non-binary secret" do
    assert {:error, :invalid_secret} = Credential.hash("")
    assert {:error, :invalid_secret} = Credential.hash(:s3cret)
    assert {:error, :invalid_secret} = Credential.hash(nil)
  end
end
