defmodule JasminEx.Dlr.HttpClient.Mint do
  @moduledoc false
  @behaviour JasminEx.Dlr.HttpClient

  alias JasminEx.Dlr.DestinationPolicy

  @default_connect_timeout 5_000
  @default_timeout 30_000
  @default_max_header_size 16_384
  @default_max_body_size 65_536

  @impl true
  def request(context, request) when is_list(context) and is_map(request) do
    deadline = now() + Keyword.get(context, :timeout, @default_timeout)

    policy_opts = [
      resolver: Keyword.get(context, :resolver, {DestinationPolicy.SystemResolver, nil}),
      allow: Keyword.get(context, :allow, []),
      allow_reserved_query: true
    ]

    with {:ok, approved} <- DestinationPolicy.approve(request.url, policy_opts),
         {:ok, target} <- request_target(request.url),
         :ok <- before_deadline(deadline) do
      perform(approved, target, request, context, deadline)
    end
  rescue
    error -> {:error, {:mint, error}}
  catch
    kind, reason -> {:error, {:mint, {kind, reason}}}
  end

  defp perform(approved, target, request, context, deadline) do
    opts = connection_options(approved, context, deadline)

    case Mint.HTTP.connect(scheme(approved.scheme), approved.peer, approved.port, opts) do
      {:ok, conn} -> request_and_close(conn, approved, target, request, context, deadline)
      {:error, reason} -> connect_error(approved.scheme, reason)
    end
  end

  defp request_and_close(conn, approved, target, request, context, deadline) do
    result =
      with :ok <- before_deadline(deadline),
           {:ok, conn, ref} <-
             Mint.HTTP.request(
               conn,
               request.method,
               target,
               request_headers(request.headers, approved.host_header),
               request.body
             ),
           :ok <- before_deadline(deadline) do
        receive_response(conn, ref, response_state(context), deadline)
      else
        {:error, conn, reason} -> normalize_error(conn, reason)
        {:error, _reason} = error -> error
      end

    result
  after
    _result = Mint.HTTP.close(conn)
  end

  defp receive_response(conn, ref, state, deadline) do
    case remaining(deadline) do
      0 ->
        {:error, :timeout}

      timeout ->
        receive_bytes(conn, ref, state, deadline, timeout)
    end
  end

  defp receive_bytes(conn, ref, state, deadline, timeout) do
    case Mint.HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        handle_responses(conn, ref, responses, state, deadline)

      {:error, conn, reason, responses} ->
        handle_receive_error(conn, ref, responses, state, reason)
    end
  end

  defp handle_receive_error(conn, ref, responses, state, receive_error) do
    case reduce_responses(ref, responses, state) do
      {:done, result} -> result
      {:error, reason} -> {:error, reason}
      {:continue, _state} -> normalize_error(conn, receive_error)
    end
  end

  defp handle_responses(conn, ref, responses, state, deadline) do
    case reduce_responses(ref, responses, state) do
      {:done, result} -> result
      {:error, reason} -> {:error, reason}
      {:continue, state} -> receive_response(conn, ref, state, deadline)
    end
  end

  defp reduce_responses(ref, responses, state) do
    Enum.reduce_while(responses, {:continue, state}, fn response, {:continue, state} ->
      case response_event(response, ref, state) do
        {:continue, state} -> {:cont, {:continue, state}}
        terminal -> {:halt, terminal}
      end
    end)
  end

  defp response_event({:status, ref, status}, ref, state) when status in 100..199 do
    {:continue, %{state | status: nil, header_size: 0, informational?: true}}
  end

  defp response_event({:status, ref, status}, ref, state) do
    {:continue, %{state | status: status, header_size: 0, informational?: false}}
  end

  defp response_event({:headers, ref, headers}, ref, %{informational?: true} = state) do
    case add_header_size(state, headers) do
      {:ok, state} -> {:continue, state}
      error -> error
    end
  end

  defp response_event({:headers, ref, headers}, ref, state) do
    with {:ok, state} <- add_header_size(state, headers),
         :ok <- declared_body_size(headers, state.max_body_size) do
      {:continue, state}
    end
  end

  defp response_event({:data, ref, chunk}, ref, state) do
    next_size = state.body_size + byte_size(chunk)

    if next_size > state.max_body_size do
      {:error, :response_body_too_large}
    else
      {:continue, %{state | body: [chunk | state.body], body_size: next_size}}
    end
  end

  defp response_event({:done, ref}, ref, %{status: status} = state)
       when is_integer(status) and status >= 200 do
    {:done, {:ok, status, state.body |> Enum.reverse() |> IO.iodata_to_binary()}}
  end

  defp response_event({:done, ref}, ref, _state), do: {:error, {:protocol, :missing_final_status}}
  defp response_event({:error, ref, reason}, ref, _state), do: protocol_error(reason)
  defp response_event(_response, _ref, state), do: {:continue, state}

  defp response_state(context) do
    %{
      status: nil,
      informational?: false,
      header_size: 0,
      max_header_size: Keyword.get(context, :max_header_size, @default_max_header_size),
      body: [],
      body_size: 0,
      max_body_size: Keyword.get(context, :max_body_size, @default_max_body_size)
    }
  end

  defp add_header_size(state, headers) do
    size =
      Enum.reduce(headers, state.header_size, fn {name, value}, total ->
        total + byte_size(name) + byte_size(value)
      end)

    if size > state.max_header_size,
      do: {:error, :response_headers_too_large},
      else: {:ok, %{state | header_size: size}}
  end

  defp declared_body_size(headers, limit) do
    Enum.reduce_while(headers, :ok, fn
      {name, value}, :ok when name in ["content-length", "Content-Length"] ->
        case Integer.parse(value) do
          {size, ""} when size > limit -> {:halt, {:error, :response_body_too_large}}
          _other -> {:cont, :ok}
        end

      _header, :ok ->
        {:cont, :ok}
    end)
  end

  defp connection_options(approved, context, deadline) do
    remaining = remaining(deadline)

    connect_timeout =
      min(Keyword.get(context, :connect_timeout, @default_connect_timeout), remaining)

    [
      hostname: approved.original_host,
      protocols: [:http1],
      mode: :passive,
      stream_headers: true,
      max_header_list_size: Keyword.get(context, :max_header_size, @default_max_header_size),
      transport_opts: transport_options(approved, context, max(connect_timeout, 1), remaining)
    ]
  end

  defp transport_options(approved, context, connect_timeout, remaining) do
    address_options =
      if tuple_size(approved.peer) == 8,
        do: [inet6: true, inet4: false],
        else: [inet6: false]

    tls_options =
      case {approved.scheme, Keyword.get(context, :cacertfile)} do
        {"https", path} when is_binary(path) -> [cacertfile: path]
        {"https", _path} -> [cacerts: :public_key.cacerts_get()]
        _other -> []
      end

    [timeout: connect_timeout, send_timeout: max(remaining, 1), send_timeout_close: true] ++
      address_options ++ tls_options
  end

  defp request_headers(headers, host) do
    headers
    |> Enum.reject(fn {name, _value} -> String.downcase(name) == "host" end)
    |> then(&[{"host", host} | &1])
  end

  defp request_target(url) do
    %URI{path: path, query: query} = URI.parse(url)
    path = if path in [nil, ""], do: "/", else: path
    {:ok, if(is_binary(query), do: path <> "?" <> query, else: path)}
  end

  defp connect_error(_scheme, %Mint.TransportError{reason: :timeout}), do: {:error, :timeout}
  defp connect_error("https", reason), do: {:error, {:tls, unwrap(reason)}}
  defp connect_error(_scheme, reason), do: {:error, {:network, unwrap(reason)}}

  defp normalize_error(_conn, %Mint.TransportError{reason: :timeout}), do: {:error, :timeout}
  defp normalize_error(_conn, %Mint.HTTPError{} = error), do: protocol_error(error)
  defp normalize_error(_conn, reason), do: {:error, {:network, unwrap(reason)}}

  defp protocol_error(%Mint.HTTPError{reason: {:max_header_list_size_exceeded, _size, _max}}),
    do: {:error, :response_headers_too_large}

  defp protocol_error(reason), do: {:error, {:protocol, unwrap(reason)}}

  defp unwrap(%{reason: reason}), do: reason
  defp unwrap(reason), do: reason

  defp scheme("http"), do: :http
  defp scheme("https"), do: :https

  defp before_deadline(deadline) do
    if remaining(deadline) > 0, do: :ok, else: {:error, :timeout}
  end

  defp remaining(deadline), do: max(deadline - now(), 0)
  defp now, do: System.monotonic_time(:millisecond)
end
