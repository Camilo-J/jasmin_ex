defmodule JasminEx.Routing.Snapshot do
  @moduledoc false

  alias JasminEx.Billing.{Clock, Fingerprint, Reservation, Tombstone}

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

  @max_id_bytes 128
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808

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
      {:ok, payload} -> decode(payload, config.clock)
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
      "version" => 3,
      "revision" => state.revision,
      "groups" =>
        Enum.map(Map.values(state.groups), &%{"gid" => &1.gid, "enabled" => &1.enabled}),
      "users" => Enum.map(Map.values(state.users), &encode_user/1),
      "routes" => Enum.map(Map.values(state.routes.routes), &encode_route/1),
      "reservations" => Enum.map(Map.values(state.reservations), &encode_reservation/1),
      "tombstones" => Enum.map(Map.values(state.tombstones), &encode_tombstone/1)
    }
  end

  defp encode_user(%{credential: cred} = user) do
    %{
      "uid" => user.uid,
      "gid" => user.gid,
      "username" => user.username,
      "enabled" => user.enabled,
      "credential" => encode_credential(cred),
      "balance_minor" => json_amount(user.balance_minor),
      "submit_quota" => json_amount(user.submit_quota),
      "smpp_credential" => encode_credential(user.smpp_credential),
      "max_bindings" => user.max_bindings
    }
  end

  defp encode_credential(nil), do: :null

  defp encode_credential(cred) do
    %{
      "algorithm" => cred.algorithm,
      "iterations" => cred.iterations,
      "salt" => Base.encode64(cred.salt),
      "digest" => Base.encode64(cred.digest)
    }
  end

  defp encode_route(route) do
    %{
      "kind" => Atom.to_string(route.kind),
      "order" => route.order,
      "connector" => %{"type" => "smpp_client", "id" => route.connector.id},
      "filters" => Enum.map(route.filters, &encode_filter/1),
      "rate_minor" => route.rate_minor,
      "precharge_percent" => route.precharge_percent
    }
  end

  defp encode_filter(%mod{} = filter) do
    {type, field} = Map.fetch!(@by_mod, mod)
    %{"type" => type, Atom.to_string(field) => Map.fetch!(filter, field)}
  end

  defp decode(payload, clock) do
    with {:ok, map} <- json(payload), :ok <- version(map), {:ok, state} <- load(map, clock) do
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

  defp version(%{"version" => version}) when version in [1, 2, 3], do: :ok
  defp version(%{"version" => _version}), do: {:error, :unsupported_version}
  defp version(_map), do: {:error, :invalid_json}

  defp load(
         %{
           "version" => 1,
           "revision" => rev,
           "groups" => groups,
           "users" => users,
           "routes" => routes
         },
         _clock
       )
       when is_integer(rev) and rev >= 0 and is_list(groups) and is_list(users) and
              is_list(routes) do
    with {:ok, state} <- reduce_state(State.new(), groups, &load_group/2),
         {:ok, state} <- reduce_state(state, users, &load_user/2),
         {:ok, state} <- reduce_state(state, routes, &load_route/2),
         do: {:ok, %{state | revision: rev}}
  end

  defp load(
         %{
           "version" => version,
           "revision" => rev,
           "groups" => groups,
           "users" => users,
           "routes" => routes,
           "reservations" => reservations,
           "tombstones" => tombstones
         },
         clock
       )
       when version in [2, 3] and is_integer(rev) and rev >= 0 and is_list(groups) and
              is_list(users) and is_list(routes) and is_list(reservations) and
              is_list(tombstones) do
    loader = if(version == 3, do: &load_user_v3/2, else: &load_user_v2/2)

    with {:ok, state} <- reduce_state(State.new(), groups, &load_group/2),
         {:ok, state} <- reduce_state(state, users, loader),
         {:ok, state} <- reduce_state(state, routes, &load_route_v2/2),
         {:ok, state} <- reduce_state(state, reservations, &load_reservation(&1, &2, clock)),
         {:ok, state} <- reduce_state(state, tombstones, &load_tombstone/2),
         do: {:ok, %{state | revision: rev}}
  end

  defp load(_map, _clock), do: {:error, :invalid_state}

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

  defp encode_reservation(%Reservation{} = reservation) do
    %{
      "bill_id" => reservation.bill_id,
      "uid" => reservation.uid,
      "fingerprint" => encode_fingerprint(reservation.fingerprint),
      "state" => "open",
      "captured_minor" => reservation.captured_minor,
      "reserved_minor" => reservation.reserved_minor,
      "refundable_minor" => reservation.refundable_minor,
      "wall_deadline_ms" => reservation.wall_deadline_ms
    }
  end

  defp encode_tombstone(%Tombstone{} = stone) do
    %{
      "bill_id" => stone.bill_id,
      "fingerprint" => encode_fingerprint(stone.fingerprint),
      "state" => Atom.to_string(stone.state)
    }
  end

  defp encode_fingerprint(%Fingerprint{version: 1, digest: digest}) do
    %{"version" => 1, "digest" => Base.encode64(digest)}
  end

  defp json_amount(nil), do: :null
  defp json_amount(amount), do: amount

  defp load_user_v2(state, %{"balance_minor" => balance, "submit_quota" => quota} = attrs) do
    with {:ok, state} <- load_user(state, attrs),
         {:ok, balance} <- decode_optional_amount(balance),
         {:ok, quota} <- decode_optional_amount(quota) do
      user = state.users[attrs["uid"]]
      State.put_user(state, %{user | balance_minor: balance, submit_quota: quota})
    else
      _error -> {:error, :invalid_state}
    end
  end

  defp load_user_v2(_state, _attrs), do: {:error, :invalid_state}

  defp load_user_v3(state, %{"smpp_credential" => smpp, "max_bindings" => max} = attrs) do
    with {:ok, state} <- load_user_v2(state, attrs),
         {:ok, smpp} <- decode_optional_credential(smpp),
         {:ok, max} <- decode_max_bindings(max) do
      user = state.users[attrs["uid"]]
      State.put_user(state, %{user | smpp_credential: smpp, max_bindings: max})
    else
      _error -> {:error, :invalid_state}
    end
  end

  defp load_user_v3(_state, _attrs), do: {:error, :invalid_state}

  defp decode_optional_credential(:null), do: {:ok, nil}
  defp decode_optional_credential(attrs), do: decode_credential(attrs)

  defp decode_max_bindings(n) when is_integer(n) and n >= 0 and n <= @max_int64, do: {:ok, n}
  defp decode_max_bindings(_n), do: {:error, :invalid_state}

  defp load_route_v2(state, %{"rate_minor" => rate, "precharge_percent" => percent} = attrs) do
    with {:ok, connector} <- decode_connector(attrs["connector"]),
         {:ok, filters} <- decode_filters(attrs["filters"]),
         {:ok, route} <-
           Route.new(
             kind: decode_kind(attrs["kind"]),
             order: attrs["order"],
             connector: connector,
             filters: filters,
             rate_minor: rate,
             precharge_percent: percent
           ),
         do: State.put_route(state, route),
         else: (_ -> {:error, :invalid_state})
  end

  defp load_route_v2(_state, _attrs), do: {:error, :invalid_state}

  defp load_reservation(state, %{"state" => "open", "uid" => uid} = attrs, clock) do
    with {:ok, bill_id} <- decode_bill_id(attrs["bill_id"]),
         true <- Map.has_key?(state.users, uid) and unique_bill?(state, bill_id),
         {:ok, fingerprint} <- decode_fingerprint(attrs["fingerprint"]),
         {:ok, captured} <- decode_amount(attrs["captured_minor"]),
         {:ok, reserved} <- decode_amount(attrs["reserved_minor"]),
         {:ok, refundable} <- decode_amount(attrs["refundable_minor"]),
         {:ok, wall} <- decode_int64(attrs["wall_deadline_ms"]),
         {:ok, monotonic} <- rehydrate_monotonic(wall, clock) do
      reservation = %Reservation{
        bill_id: bill_id,
        uid: uid,
        fingerprint: fingerprint,
        state: :open,
        captured_minor: captured,
        reserved_minor: reserved,
        refundable_minor: refundable,
        wall_deadline_ms: wall,
        monotonic_deadline_ms: monotonic
      }

      {:ok, %{state | reservations: Map.put(state.reservations, bill_id, reservation)}}
    else
      _error -> {:error, :invalid_state}
    end
  end

  defp load_reservation(_state, _attrs, _clock), do: {:error, :invalid_state}

  defp load_tombstone(state, %{
         "bill_id" => bill_id,
         "fingerprint" => fingerprint,
         "state" => state_name
       }) do
    with {:ok, bill_id} <- decode_bill_id(bill_id),
         true <- unique_bill?(state, bill_id),
         {:ok, fingerprint} <- decode_fingerprint(fingerprint),
         {:ok, stone_state} <- decode_tombstone_state(state_name) do
      stone = %Tombstone{bill_id: bill_id, fingerprint: fingerprint, state: stone_state}
      {:ok, %{state | tombstones: Map.put(state.tombstones, bill_id, stone)}}
    else
      _error -> {:error, :invalid_state}
    end
  end

  defp load_tombstone(_state, _attrs), do: {:error, :invalid_state}

  defp unique_bill?(state, bill_id) do
    not Map.has_key?(state.reservations, bill_id) and not Map.has_key?(state.tombstones, bill_id)
  end

  defp decode_bill_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_id_bytes,
       do: {:ok, id}

  defp decode_bill_id(_id), do: {:error, :invalid_state}

  defp decode_fingerprint(%{"version" => 1, "digest" => digest}) when is_binary(digest) do
    with {:ok, raw} <- Base.decode64(digest),
         true <- byte_size(raw) == 32 do
      {:ok, %Fingerprint{version: 1, digest: raw}}
    else
      _error -> {:error, :invalid_state}
    end
  end

  defp decode_fingerprint(_attrs), do: {:error, :invalid_state}

  defp decode_tombstone_state("settled_ok"), do: {:ok, :settled_ok}
  defp decode_tombstone_state("settled_non_ok"), do: {:ok, :settled_non_ok}
  defp decode_tombstone_state("expired"), do: {:ok, :expired}
  defp decode_tombstone_state(_state), do: {:error, :invalid_state}

  defp decode_optional_amount(:null), do: {:ok, nil}
  defp decode_optional_amount(amount), do: decode_amount(amount)

  defp decode_amount(amount) when is_integer(amount) and amount >= 0 and amount <= @max_int64,
    do: {:ok, amount}

  defp decode_amount(_amount), do: {:error, :invalid_state}

  defp decode_int64(amount)
       when is_integer(amount) and amount >= @min_int64 and amount <= @max_int64,
       do: {:ok, amount}

  defp decode_int64(_amount), do: {:error, :invalid_state}

  defp rehydrate_monotonic(wall_deadline_ms, clock) do
    decode_int64(Clock.monotonic_ms(clock) + (wall_deadline_ms - Clock.wall_ms(clock)))
  end
end
