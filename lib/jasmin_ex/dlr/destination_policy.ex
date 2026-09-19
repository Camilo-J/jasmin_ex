defmodule JasminEx.Dlr.DestinationPolicy do
  @moduledoc false
  import Bitwise

  @reserved_query_fields ~w(id level message_status connector id_smsc sub dlvrd subdate donedate err text)

  defmodule SystemResolver do
    @moduledoc false

    def resolve(_context, host) do
      case :inet.getaddrs(String.to_charlist(host), :inet) do
        {:ok, []} ->
          {:error, :dns_empty}

        {:ok, addresses} ->
          {:ok, addresses}

        {:error, _reason} ->
          case :inet.getaddrs(String.to_charlist(host), :inet6) do
            {:ok, []} -> {:error, :dns_empty}
            result -> result
          end
      end
    end
  end

  @spec validate_url(binary(), keyword()) :: {:ok, URI.t()} | {:error, atom()}
  def validate_url(url, opts \\ []) when is_binary(url) do
    with :ok <- controls(url),
         %URI{} = uri <- URI.parse(url),
         :ok <- scheme(uri),
         :ok <- authority(uri),
         :ok <- port(uri),
         :ok <- query(uri, opts) do
      {:ok, uri}
    else
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :invalid_url}
  end

  @spec approve(binary(), keyword()) :: {:ok, map()} | {:error, atom()}
  def approve(url, opts \\ []) do
    with {:ok, uri} <- validate_url(url, opts),
         {:ok, addresses} <-
           resolve(uri.host, Keyword.get(opts, :resolver, {SystemResolver, nil})),
         :ok <- allowed_addresses(uri.host, addresses, Keyword.get(opts, :allow, [])) do
      peer = hd(addresses)
      port = uri.port || default_port(uri.scheme)

      {:ok,
       %{
         scheme: uri.scheme,
         original_host: uri.host,
         port: port,
         peer: peer,
         host_header: host_header(uri.host, port, uri.scheme),
         request_url: pinned_url(uri, peer, port)
       }}
    end
  end

  defp controls(url) do
    if String.to_charlist(url) |> Enum.any?(&(&1 < 32 or &1 == 127)),
      do: {:error, :control_character},
      else: :ok
  end

  defp scheme(%URI{scheme: scheme}) when scheme in ["http", "https"], do: :ok
  defp scheme(_uri), do: {:error, :unsupported_scheme}

  defp authority(%URI{host: host, userinfo: nil, fragment: nil})
       when is_binary(host) and host != "",
       do: :ok

  defp authority(%URI{userinfo: userinfo}) when not is_nil(userinfo), do: {:error, :userinfo}
  defp authority(%URI{fragment: fragment}) when not is_nil(fragment), do: {:error, :fragment}
  defp authority(_uri), do: {:error, :invalid_host}

  defp port(%URI{port: nil}), do: :ok
  defp port(%URI{port: port}) when port in 1..65_535, do: :ok
  defp port(_uri), do: {:error, :invalid_port}

  defp query(uri, opts) do
    if Keyword.get(opts, :allow_reserved_query, false), do: :ok, else: checked_query(uri)
  end

  defp checked_query(%URI{query: nil}), do: :ok

  defp checked_query(%URI{query: query}) do
    if query
       |> URI.query_decoder()
       |> Enum.any?(fn {key, _value} -> key in @reserved_query_fields end),
       do: {:error, :reserved_query_field},
       else: :ok
  end

  defp resolve(host, {module, context}) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> {:ok, [address]}
      {:error, :einval} -> module.resolve(context, host)
    end
  end

  defp allowed_addresses(_host, [], _allow), do: {:error, :dns_empty}

  defp allowed_addresses(host, addresses, allow) do
    if Enum.all?(addresses, &(public?(&1) or {host, &1} in allow)),
      do: :ok,
      else: {:error, :forbidden_address}
  end

  defp public?({a, _b, _c, _d}) when a in [0, 10, 127] or a >= 224, do: false
  defp public?({100, b, _c, _d}) when b in 64..127, do: false
  defp public?({169, 254, _c, _d}), do: false
  defp public?({172, b, _c, _d}) when b in 16..31, do: false
  defp public?({192, 0, 0, _d}), do: false
  defp public?({192, 0, 2, _d}), do: false
  defp public?({192, 168, _c, _d}), do: false
  defp public?({198, b, _c, _d}) when b in [18, 19, 51], do: false
  defp public?({203, 0, 113, _d}), do: false
  defp public?({_a, _b, _c, _d}), do: true

  defp public?({0, 0, 0, 0, 0, 0, 0, 0}), do: false
  defp public?({0, 0, 0, 0, 0, 0, 0, 1}), do: false
  defp public?({a, _b, _c, _d, _e, _f, _g, _h}) when (a &&& 0xFE00) == 0xFC00, do: false
  defp public?({a, _b, _c, _d, _e, _f, _g, _h}) when (a &&& 0xFFC0) == 0xFEC0, do: false
  defp public?({a, _b, _c, _d, _e, _f, _g, _h}) when (a &&& 0xFFC0) == 0xFE80, do: false
  defp public?({a, _b, _c, _d, _e, _f, _g, _h}) when (a &&& 0xFF00) == 0xFF00, do: false
  defp public?({0x2001, 0xDB8, _c, _d, _e, _f, _g, _h}), do: false

  defp public?({0, 0, 0, 0, 0, 0xFFFF, high, low}) do
    public?({high >>> 8, high &&& 255, low >>> 8, low &&& 255})
  end

  defp public?({_a, _b, _c, _d, _e, _f, _g, _h}), do: true
  defp public?(_address), do: false

  defp default_port("http"), do: 80
  defp default_port("https"), do: 443

  defp host_header(host, port, scheme) do
    if port == default_port(scheme), do: host, else: "#{host}:#{port}"
  end

  defp pinned_url(uri, peer, port) do
    peer_host = peer |> :inet.ntoa() |> List.to_string()
    URI.to_string(%{uri | host: peer_host, port: port, userinfo: nil, fragment: nil})
  end
end
