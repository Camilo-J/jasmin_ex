defmodule JasminEx.Routing.User do
  @moduledoc false

  alias JasminEx.Routing.Credential
  alias JasminEx.Routing.Group

  @uid_pattern ~r/^[A-Za-z0-9_-]{1,16}$/
  @username_pattern ~r/^[A-Za-z0-9_-]{1,15}$/

  @enforce_keys [:uid, :gid, :username, :credential, :enabled]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          uid: String.t(),
          gid: String.t(),
          username: String.t(),
          credential: Credential.t(),
          enabled: boolean()
        }

  @spec new(keyword()) ::
          {:ok, t()}
          | {:error,
             :unknown_group
             | :invalid_uid
             | :invalid_username
             | :invalid_secret
             | :invalid_enabled}
  def new(attrs) when is_list(attrs) do
    with {:ok, group} <- require_group(Keyword.get(attrs, :group)),
         {:ok, uid} <- validate_uid(Keyword.get(attrs, :uid)),
         {:ok, username} <- validate_username(Keyword.get(attrs, :username)),
         {:ok, enabled} <- validate_enabled(Keyword.get(attrs, :enabled, true)),
         {:ok, credential} <- Credential.hash(Keyword.get(attrs, :secret)) do
      {:ok,
       %__MODULE__{
         uid: uid,
         gid: group.gid,
         username: username,
         credential: credential,
         enabled: enabled
       }}
    end
  end

  def new(_attrs), do: {:error, :unknown_group}

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
end
