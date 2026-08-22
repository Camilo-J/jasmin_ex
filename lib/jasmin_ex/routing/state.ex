defmodule JasminEx.Routing.State do
  @moduledoc false

  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.RouteTable
  alias JasminEx.Routing.User

  defstruct groups: %{}, users: %{}, routes: %RouteTable{}, revision: 0

  @type t :: %__MODULE__{
          groups: %{optional(String.t()) => Group.t()},
          users: %{optional(String.t()) => User.t()},
          routes: RouteTable.t(),
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

  @spec put_route(t(), Route.t()) :: {:ok, t()} | {:error, :invalid_order}
  def put_route(%__MODULE__{} = state, %Route{} = route) do
    with {:ok, table} <- RouteTable.put(state.routes, route) do
      {:ok, %{state | routes: table}}
    end
  end

  @spec delete_group(t(), term()) :: {:ok, t()} | {:error, :unknown_group}
  def delete_group(%__MODULE__{} = state, gid) when is_binary(gid) do
    drop_group(state, Map.pop(state.groups, gid))
  end

  def delete_group(%__MODULE__{}, _gid), do: {:error, :unknown_group}

  defp drop_group(_state, {nil, _groups}), do: {:error, :unknown_group}

  defp drop_group(state, {%Group{gid: gid}, groups}) do
    users = Map.reject(state.users, fn {_uid, user} -> user.gid == gid end)
    {:ok, %{state | groups: groups, users: users}}
  end

  defp username_taken?(users, username),
    do: Enum.any?(users, fn {_uid, user} -> user.username == username end)
end
