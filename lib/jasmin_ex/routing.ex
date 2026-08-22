defmodule JasminEx.Routing do
  @moduledoc """
  Public context for MT routing identity, eligibility, and resolution.
  """

  alias JasminEx.Routing.Credential
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Router
  alias JasminEx.Routing.State
  alias JasminEx.Routing.User

  defdelegate snapshot(server), to: Router
  defdelegate put_group(server, attrs), to: Router
  defdelegate put_user(server, attrs), to: Router
  defdelegate put_route(server, attrs), to: Router
  defdelegate delete_group(server, gid), to: Router

  @spec authenticate(GenServer.server(), String.t(), term()) ::
          {:ok, User.t()} | {:error, :invalid_credentials | :user_disabled | :group_disabled}
  def authenticate(server, username, secret) do
    authenticate_snapshot(Router.snapshot(server), username, secret)
  end

  @spec authenticate_snapshot(State.t(), String.t(), term()) ::
          {:ok, User.t()} | {:error, :invalid_credentials | :user_disabled | :group_disabled}
  def authenticate_snapshot(%State{} = state, username, secret) do
    case find_user(state.users, username) do
      nil -> {:error, :invalid_credentials}
      user -> eligibility(state, user, secret)
    end
  end

  defp eligibility(state, user, secret) do
    cond do
      not Credential.verify(user.credential, secret) -> {:error, :invalid_credentials}
      not user.enabled -> {:error, :user_disabled}
      not group_enabled?(state, user.gid) -> {:error, :group_disabled}
      true -> {:ok, user}
    end
  end

  defp group_enabled?(%{groups: groups}, gid), do: match?(%Group{enabled: true}, groups[gid])

  defp find_user(users, username) do
    Enum.find_value(users, fn {_uid, user} -> user.username == username && user end)
  end
end
