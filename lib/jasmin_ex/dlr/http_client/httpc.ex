defmodule JasminEx.Dlr.HttpClient.Httpc do
  @moduledoc false
  @behaviour JasminEx.Dlr.HttpClient

  alias JasminEx.Dlr.DestinationPolicy

  @impl true
  def request(context, request) when is_list(context) and is_map(request) do
    policy_opts = [
      resolver: Keyword.get(context, :resolver, {DestinationPolicy.SystemResolver, nil}),
      allow: Keyword.get(context, :allow, []),
      allow_reserved_query: true
    ]

    with {:ok, approved} <- DestinationPolicy.approve(request.url, policy_opts),
         :ok <- ensure_profile(Keyword.fetch!(context, :profile)) do
      perform(approved, request, context)
    end
  end

  defp perform(approved, request, context) do
    method = if request.method == "GET", do: :get, else: :post
    headers = request_headers(request.headers, approved.host_header)
    http_options = request_options(approved, context)
    response_options = [sync: false, stream: {:self, :once}, receiver: self()]
    profile = Keyword.fetch!(context, :profile)

    result =
      :httpc.request(
        method,
        request_tuple(method, approved.request_url, headers, request.body),
        http_options,
        response_options,
        profile
      )

    await(result, profile, context)
  rescue
    error -> {:error, {:httpc, error}}
  end

  defp ensure_profile(profile) do
    _ = Application.ensure_all_started(:inets)
    _ = Application.ensure_all_started(:ssl)

    case :inets.start(:httpc, profile: profile) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp request_options(approved, context) do
    base = [
      timeout: Keyword.get(context, :timeout, 30_000),
      connect_timeout: Keyword.get(context, :connect_timeout, 5_000),
      autoredirect: false,
      autoretry: 0
    ]

    if approved.scheme == "https" do
      Keyword.put(base, :ssl, ssl_options(approved.original_host, context))
    else
      base
    end
  end

  defp ssl_options(host, context) do
    trust =
      case Keyword.get(context, :cacertfile) do
        path when is_binary(path) -> [cacerts: certificate_authorities(path)]
        _other -> [cacerts: :public_key.cacerts_get()]
      end

    [
      verify: :verify_peer,
      server_name_indication: String.to_charlist(host),
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ] ++ trust
  end

  defp certificate_authorities(path) do
    path
    |> File.read!()
    |> :public_key.pem_decode()
    |> Enum.flat_map(fn
      {:Certificate, der, :not_encrypted} -> [der]
      _entry -> []
    end)
  end

  defp request_headers(headers, host) do
    [{~c"host", String.to_charlist(host)} | encode_headers(headers)]
  end

  defp encode_headers(headers) do
    Enum.map(headers, fn {name, value} ->
      {String.to_charlist(name), String.to_charlist(value)}
    end)
  end

  defp request_tuple(:get, url, headers, _body),
    do: {String.to_charlist(url), headers}

  defp request_tuple(:post, url, headers, body) do
    content_type =
      headers
      |> Enum.find_value(~c"application/x-www-form-urlencoded", fn
        {~c"content-type", value} -> value
        _other -> nil
      end)

    {String.to_charlist(url), headers, content_type, body}
  end

  defp await({:ok, request_id}, profile, context) do
    deadline = System.monotonic_time(:millisecond) + Keyword.get(context, :timeout, 30_000)
    receive_response(request_id, profile, context, deadline)
  end

  defp await({:error, reason}, _profile, _context), do: {:error, reason}

  defp receive_response(request_id, profile, context, deadline) do
    receive do
      {:http, {^request_id, :stream_start, headers, handler}} ->
        if headers_too_large?(headers, context) or declared_body_too_large?(headers, context) do
          cancel(request_id, profile)
          {:error, :response_too_large}
        else
          :ok = :httpc.stream_next(handler)

          receive_body(
            request_id,
            handler,
            profile,
            context,
            deadline,
            [],
            0,
            stream_status(headers)
          )
        end

      {:http, {^request_id, {{_version, status, _reason}, headers, body}}} ->
        normalize_response(status, headers, body, context)

      {:http, {^request_id, {:error, reason}}} ->
        {:error, reason}
    after
      remaining(deadline) ->
        cancel(request_id, profile)
        {:error, :timeout}
    end
  end

  defp receive_body(request_id, handler, profile, context, deadline, chunks, size, status) do
    receive do
      {:http, {^request_id, :stream, chunk}} ->
        next_size = size + byte_size(chunk)

        if next_size > Keyword.get(context, :max_body_size, 65_536) do
          cancel(request_id, profile)
          {:error, :response_body_too_large}
        else
          :ok = :httpc.stream_next(handler)

          receive_body(
            request_id,
            handler,
            profile,
            context,
            deadline,
            [chunk | chunks],
            next_size,
            status
          )
        end

      {:http, {^request_id, :stream_end, trailers}} ->
        if headers_too_large?(trailers, context) do
          {:error, :response_headers_too_large}
        else
          {:ok, status, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
        end

      {:http, {^request_id, {:error, reason}}} ->
        {:error, reason}
    after
      remaining(deadline) ->
        cancel(request_id, profile)
        {:error, :timeout}
    end
  end

  defp stream_status(headers) do
    if Enum.any?(headers, fn {name, _value} ->
         String.downcase(List.to_string(name)) == "content-range"
       end),
       do: 206,
       else: 200
  end

  defp normalize_response(status, headers, body, context) do
    body = IO.iodata_to_binary(body)

    cond do
      headers_too_large?(headers, context) ->
        {:error, :response_headers_too_large}

      byte_size(body) > Keyword.get(context, :max_body_size, 65_536) ->
        {:error, :response_body_too_large}

      true ->
        {:ok, status, body}
    end
  end

  defp headers_too_large?(headers, context) do
    size =
      Enum.reduce(headers, 0, fn {name, value}, total ->
        total + IO.iodata_length(name) + IO.iodata_length(value)
      end)

    size > Keyword.get(context, :max_header_size, 16_384)
  end

  defp declared_body_too_large?(headers, context) do
    limit = Keyword.get(context, :max_body_size, 65_536)

    Enum.any?(headers, fn {name, value} ->
      String.downcase(List.to_string(name)) == "content-length" and
        content_length(value) > limit
    end)
  end

  defp content_length(value) do
    value |> IO.iodata_to_binary() |> String.to_integer()
  rescue
    _error -> 0
  end

  defp remaining(deadline),
    do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp cancel(request_id, profile) do
    _result = :httpc.cancel_request(request_id, profile)
    :ok
  end
end
