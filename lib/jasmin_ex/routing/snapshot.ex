defmodule JasminEx.Routing.Snapshot do
  @moduledoc false

  alias JasminEx.Routing.{
    Config,
    ConnectorRef,
    Credential,
    FileOps,
    Filter,
    Group,
    Route,
    State,
    User
  }

  @filters %{
    "user" => {Filter.User, :uid},
    "group" => {Filter.Group, :gid},
    "source" => {Filter.Source, :address},
    "destination" => {Filter.Destination, :address},
    "content" => {Filter.Content, :body},
    "tags_all" => {Filter.TagsAll, :tags}
  }

  @by_mod Map.new(@filters, fn {type, {mod, field}} -> {mod, {type, field}} end)

  def write(%State{} = state, %Config{} = config) do
    payload = state |> encode() |> :json.encode() |> IO.iodata_to_binary()
    atomic_write(ops(config), config.snapshot_path, payload)
  end

  def restore(%Config{} = config) do
    case ops(config).read(config.snapshot_path) do
      {:error, :enoent} -> {:ok, State.new()}
      {:ok, payload} -> decode(payload)
      {:error, reason} -> {:error, {:restore_failed, reason}}
    end
  end

  defp ops(%Config{file_ops: nil}), do: FileOps
  defp ops(%Config{file_ops: module}), do: module

  defp atomic_write(ops, path, payload) do
    dir = Path.dirname(path)
    tmp = path <> ".#{System.unique_integer([:positive])}.tmp"

    with :ok <- ops.mkdir_p(dir),
         :ok <- ops.chmod(dir, 0o700),
         :ok <- ops.write(tmp, payload),
         :ok <- ops.chmod(tmp, 0o600),
         :ok <- ops.fsync(tmp),
         :ok <- ops.rename(tmp, path),
         :ok <- ops.chmod(path, 0o600),
         do: :ok,
         else: ({:error, _} -> {:error, :snapshot_failed})
  end

  defp encode(state) do
    %{
      "version" => 1,
      "revision" => state.revision,
      "groups" =>
        Enum.map(Map.values(state.groups), &%{"gid" => &1.gid, "enabled" => &1.enabled}),
      "users" => Enum.map(Map.values(state.users), &encode_user/1),
      "routes" => Enum.map(Map.values(state.routes.routes), &encode_route/1)
    }
  end

  defp encode_user(%{credential: cred} = user) do
    %{
      "uid" => user.uid,
      "gid" => user.gid,
      "username" => user.username,
      "enabled" => user.enabled,
      "credential" => %{
        "algorithm" => cred.algorithm,
        "iterations" => cred.iterations,
        "salt" => Base.encode64(cred.salt),
        "digest" => Base.encode64(cred.digest)
      }
    }
  end

  defp encode_route(route) do
    %{
      "kind" => Atom.to_string(route.kind),
      "order" => route.order,
      "connector" => %{"type" => "smpp_client", "id" => route.connector.id},
      "filters" => Enum.map(route.filters, &encode_filter/1)
    }
  end

  defp encode_filter(%mod{} = filter) do
    {type, field} = Map.fetch!(@by_mod, mod)
    %{"type" => type, Atom.to_string(field) => Map.fetch!(filter, field)}
  end

  defp decode(payload) do
    with {:ok, map} <- json(payload), :ok <- version(map), {:ok, state} <- load(map) do
      {:ok, state}
    else
      {:error, reason} -> {:error, {:restore_failed, reason}}
    end
  end

  defp json(payload) do
    {:ok, :json.decode(payload)}
  rescue
    _error -> {:error, :invalid_json}
  end

  defp version(%{"version" => 1}), do: :ok
  defp version(%{"version" => _version}), do: {:error, :unsupported_version}
  defp version(_map), do: {:error, :invalid_json}

  defp load(%{"revision" => rev, "groups" => groups, "users" => users, "routes" => routes})
       when is_integer(rev) and rev >= 0 and is_list(groups) and is_list(users) and
              is_list(routes) do
    with {:ok, state} <- reduce_state(State.new(), groups, &load_group/2),
         {:ok, state} <- reduce_state(state, users, &load_user/2),
         {:ok, state} <- reduce_state(state, routes, &load_route/2),
         do: {:ok, %{state | revision: rev}}
  end

  defp load(_map), do: {:error, :invalid_state}

  defp reduce_state(state, items, fun) do
    Enum.reduce_while(items, {:ok, state}, fn item, {:ok, acc} ->
      case fun.(acc, item) do
        {:ok, next} -> {:cont, {:ok, next}}
        error -> {:halt, error}
      end
    end)
  end

  defp load_group(state, %{"gid" => gid, "enabled" => enabled}) do
    case Group.new(gid: gid, enabled: enabled) do
      {:ok, group} -> State.put_group(state, group)
      _error -> {:error, :invalid_state}
    end
  end

  defp load_group(_state, _attrs), do: {:error, :invalid_state}

  defp load_user(state, %{
         "uid" => uid,
         "gid" => gid,
         "username" => username,
         "enabled" => enabled,
         "credential" => cred
       })
       when is_binary(uid) and is_binary(username) and is_boolean(enabled) do
    with {:ok, group} <- Map.fetch(state.groups, gid),
         {:ok, cred} <- decode_credential(cred),
         true <-
           Regex.match?(~r/^[A-Za-z0-9_-]{1,16}$/, uid) and
             Regex.match?(~r/^[A-Za-z0-9_-]{1,15}$/, username),
         do:
           State.put_user(state, %User{
             uid: uid,
             gid: group.gid,
             username: username,
             credential: cred,
             enabled: enabled
           }),
         else: (_ -> {:error, :invalid_state})
  end

  defp load_user(_state, _attrs), do: {:error, :invalid_state}

  defp decode_credential(%{
         "algorithm" => "pbkdf2-hmac-sha256-v1",
         "iterations" => 600_000,
         "salt" => salt,
         "digest" => digest
       }) do
    with {:ok, salt} <- Base.decode64(to_string(salt)),
         {:ok, digest} <- Base.decode64(to_string(digest)),
         true <- byte_size(salt) == 16 and byte_size(digest) == 32,
         do:
           {:ok,
            %Credential{
              algorithm: "pbkdf2-hmac-sha256-v1",
              iterations: 600_000,
              salt: salt,
              digest: digest
            }},
         else: (_ -> {:error, :invalid_state})
  end

  defp decode_credential(_attrs), do: {:error, :invalid_state}

  defp load_route(state, %{
         "kind" => kind,
         "order" => order,
         "connector" => connector,
         "filters" => filters
       }) do
    with {:ok, connector} <- decode_connector(connector),
         {:ok, filters} <- decode_filters(filters),
         {:ok, route} <-
           Route.new(
             kind: decode_kind(kind),
             order: order,
             connector: connector,
             filters: filters
           ),
         do: State.put_route(state, route),
         else: (_ -> {:error, :invalid_state})
  end

  defp load_route(_state, _attrs), do: {:error, :invalid_state}

  defp decode_kind("static"), do: :static
  defp decode_kind("default"), do: :default
  defp decode_kind(_kind), do: :invalid
  defp decode_connector(%{"type" => "smpp_client", "id" => id}), do: ConnectorRef.new(id)
  defp decode_connector(_attrs), do: {:error, :invalid_state}

  defp decode_filters(filters) when is_list(filters) do
    decoded = Enum.map(filters, &decode_filter/1)

    if Enum.all?(decoded, &match?({:ok, _}, &1)),
      do: {:ok, Enum.map(decoded, fn {:ok, filter} -> filter end)},
      else: {:error, :invalid_state}
  end

  defp decode_filters(_filters), do: {:error, :invalid_state}

  defp decode_filter(%{"type" => type} = map) do
    case Map.get(@filters, type) do
      {mod, field} -> mod.new([{field, map[Atom.to_string(field)]}])
      nil -> {:error, :invalid_state}
    end
  end

  defp decode_filter(_attrs), do: {:error, :invalid_state}
end
