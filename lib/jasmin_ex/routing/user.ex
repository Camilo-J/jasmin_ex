defmodule JasminEx.Routing.User do
  @moduledoc false

  alias JasminEx.Routing.Credential
  alias JasminEx.Routing.Group

  @uid_pattern ~r/^[A-Za-z0-9_-]{1,16}$/
  @username_pattern ~r/^[A-Za-z0-9_-]{1,15}$/

  @enforce_keys [:uid, :gid, :username, :credential, :enabled]
  defstruct @enforce_keys ++
              [
                balance_minor: nil,
                submit_quota: nil,
                smpp_credential: nil,
                max_bindings: 0,
                set_dlr_level: true,
                http_set_dlr_method: true
              ]

  @type t :: %__MODULE__{
          uid: String.t(),
          gid: String.t(),
          username: String.t(),
          credential: Credential.t(),
          enabled: boolean(),
          balance_minor: non_neg_integer() | nil,
          submit_quota: non_neg_integer() | nil,
          smpp_credential: Credential.t() | nil,
          max_bindings: non_neg_integer(),
          set_dlr_level: boolean(),
          http_set_dlr_method: boolean()
        }

  @max_int64 9_223_372_036_854_775_807

  @spec new(keyword()) ::
          {:ok, t()}
          | {:error,
             :unknown_group
             | :invalid_uid
             | :invalid_username
             | :invalid_secret
             | :invalid_enabled
             | :invalid_amount
             | :amount_overflow
             | :invalid_dlr_permission}
  def new(attrs) when is_list(attrs) do
    with {:ok, group} <- require_group(Keyword.get(attrs, :group)),
         {:ok, uid} <- validate_uid(Keyword.get(attrs, :uid)),
         {:ok, username} <- validate_username(Keyword.get(attrs, :username)),
         {:ok, enabled} <- validate_enabled(Keyword.get(attrs, :enabled, true)),
         {:ok, credential} <- Credential.hash(Keyword.get(attrs, :secret)),
         {:ok, balance_minor} <- validate_optional_amount(Keyword.get(attrs, :balance_minor)),
         {:ok, submit_quota} <- validate_optional_amount(Keyword.get(attrs, :submit_quota)),
         {:ok, set_dlr_level} <-
           validate_dlr_permission(Keyword.get(attrs, :set_dlr_level, true)),
         {:ok, http_set_dlr_method} <-
           validate_dlr_permission(Keyword.get(attrs, :http_set_dlr_method, true)) do
      {:ok,
       %__MODULE__{
         uid: uid,
         gid: group.gid,
         username: username,
         credential: credential,
         enabled: enabled,
         balance_minor: balance_minor,
         submit_quota: submit_quota,
         set_dlr_level: set_dlr_level,
         http_set_dlr_method: http_set_dlr_method
       }}
    end
  end

  def new(_attrs), do: {:error, :unknown_group}

  @spec set_smpp_secret(t(), term()) :: {:ok, t()} | {:error, :invalid_secret}
  def set_smpp_secret(%__MODULE__{} = user, secret) do
    with {:ok, credential} <- Credential.hash(secret) do
      {:ok, %{user | smpp_credential: credential}}
    end
  end

  @spec set_max_bindings(t(), term()) :: {:ok, t()} | {:error, :invalid_max_bindings}
  def set_max_bindings(%__MODULE__{} = user, limit)
      when is_integer(limit) and limit >= 0 and limit <= @max_int64 do
    {:ok, %{user | max_bindings: limit}}
  end

  def set_max_bindings(%__MODULE__{}, _limit), do: {:error, :invalid_max_bindings}

  @spec set_dlr_level(t(), term()) :: {:ok, t()} | {:error, :invalid_dlr_permission}
  def set_dlr_level(%__MODULE__{} = user, value) when is_boolean(value) do
    {:ok, %{user | set_dlr_level: value}}
  end

  def set_dlr_level(%__MODULE__{}, _value), do: {:error, :invalid_dlr_permission}

  @spec set_http_set_dlr_method(t(), term()) :: {:ok, t()} | {:error, :invalid_dlr_permission}
  def set_http_set_dlr_method(%__MODULE__{} = user, value) when is_boolean(value) do
    {:ok, %{user | http_set_dlr_method: value}}
  end

  def set_http_set_dlr_method(%__MODULE__{}, _value), do: {:error, :invalid_dlr_permission}

  defp require_group(%Group{} = group), do: {:ok, group}
  defp require_group(_group), do: {:error, :unknown_group}

  defp validate_uid(uid) when is_binary(uid) do
    if Regex.match?(@uid_pattern, uid), do: {:ok, uid}, else: {:error, :invalid_uid}
  end

  defp validate_uid(_uid), do: {:error, :invalid_uid}

  defp validate_username(username) when is_binary(username) do
    if Regex.match?(@username_pattern, username),
      do: {:ok, username},
      else: {:error, :invalid_username}
  end

  defp validate_username(_username), do: {:error, :invalid_username}

  defp validate_enabled(enabled) when is_boolean(enabled), do: {:ok, enabled}
  defp validate_enabled(_enabled), do: {:error, :invalid_enabled}

  defp validate_dlr_permission(value) when is_boolean(value), do: {:ok, value}
  defp validate_dlr_permission(_value), do: {:error, :invalid_dlr_permission}

  defp validate_optional_amount(nil), do: {:ok, nil}

  defp validate_optional_amount(amount) when is_integer(amount) and amount < 0,
    do: {:error, :invalid_amount}

  defp validate_optional_amount(amount) when is_integer(amount) and amount > @max_int64,
    do: {:error, :amount_overflow}

  defp validate_optional_amount(amount) when is_integer(amount), do: {:ok, amount}
  defp validate_optional_amount(_amount), do: {:error, :invalid_amount}
end
