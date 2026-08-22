defmodule JasminEx.Routing.State do
  @moduledoc false

  alias JasminEx.Routing.Group
  alias JasminEx.Routing.User

  defstruct groups: %{}, users: %{}, routes: %{}, revision: 0

  @type t :: %__MODULE__{
          groups: %{optional(String.t()) => Group.t()},
          users: %{optional(String.t()) => User.t()},
          routes: map(),
          revision: non_neg_integer()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec put_group(t(), Group.t()) :: {:ok, t()}
  def put_group(%__MODULE__{} = state, %Group{gid: gid} = group) do
    {:ok, %{state | groups: Map.put(state.groups, gid, group)}}
  end

  @spec put_user(t(), User.t()) :: {:ok, t()} | {:error, :unknown_group | :duplicate_username}
  def put_user(%__MODULE__{} = state, %User{uid: uid, gid: gid, username: username} = user) do
    cond do
      not Map.has_key?(state.groups, gid) ->
        {:error, :unknown_group}

      username_taken?(state.users, username) ->
        {:error, :duplicate_username}

      true ->
        {:ok, %{state | users: Map.put(state.users, uid, user)}}
    end
  end

  defp username_taken?(users, username),
    do: Enum.any?(users, fn {_uid, user} -> user.username == username end)
end
