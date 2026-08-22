defmodule JasminEx.Routing.Credential do
  @moduledoc false

  @algorithm "pbkdf2-hmac-sha256-v1"
  @iterations 600_000
  @salt_size 16
  @digest_size 32

  @enforce_keys [:algorithm, :iterations, :salt, :digest]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          algorithm: String.t(),
          iterations: pos_integer(),
          salt: binary(),
          digest: binary()
        }

  @spec hash(term()) :: {:ok, t()} | {:error, :invalid_secret}
  def hash(secret) when is_binary(secret) and secret != "" do
    salt = :crypto.strong_rand_bytes(@salt_size)
    digest = derive(secret, salt, @iterations)

    {:ok,
     %__MODULE__{
       algorithm: @algorithm,
       iterations: @iterations,
       salt: salt,
       digest: digest
     }}
  end

  def hash(_secret), do: {:error, :invalid_secret}

  @spec verify(t(), term()) :: boolean()
  def verify(%__MODULE__{} = credential, secret) when is_binary(secret) do
    digest = derive(secret, credential.salt, credential.iterations)
    :crypto.hash_equals(credential.digest, digest)
  end

  def verify(%__MODULE__{}, _secret), do: false

  defp derive(secret, salt, iterations),
    do: :crypto.pbkdf2_hmac(:sha256, secret, salt, iterations, @digest_size)
end

defimpl Inspect, for: JasminEx.Routing.Credential do
  def inspect(%JasminEx.Routing.Credential{algorithm: algorithm, iterations: iterations}, _opts) do
    "#JasminEx.Routing.Credential<algorithm: #{inspect(algorithm)}, iterations: #{iterations}, REDACTED>"
  end
end
