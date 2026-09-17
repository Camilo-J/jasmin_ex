defmodule JasminEx.HttpApi.Router do
  @moduledoc false

  use Plug.Router

  alias JasminEx.Billing.Queries
  alias JasminEx.Dlr.Request, as: DlrRequest
  alias JasminEx.HttpApi.Metrics
  alias JasminEx.HttpApi.Response
  alias JasminEx.MtSubmitPipeline
  alias JasminEx.Routing
  alias JasminEx.Routing.Routable

  @send_keys ~w(username password to from content hex-content coding dlr dlr-url dlr-level dlr-method)
  @rate_keys ~w(username password to from)
  @balance_keys ~w(username password)

  plug(:match)
  plug(:dispatch)

  def init(opts) when is_map(opts), do: opts

  def call(conn, opts) do
    conn
    |> Plug.Conn.put_private(:http_api, opts)
    |> super(opts)
  end

  get "/ping" do
    finish(conn, :ping, Response.ok("pong"))
  end

  get "/metrics" do
    body = Metrics.scrape(opts(conn).metrics)
    finish(conn, :metrics, Response.ok(body))
  end

  get "/send" do
    finish(conn, :send, Response.error(:method_not_allowed))
  end

  post "/send" do
    {conn, result} = send_request(conn)
    finish(conn, :send, Response.from(result))
  end

  post "/rate" do
    {conn, result} = rate_request(conn)
    finish(conn, :rate, Response.from(result))
  end

  post "/balance" do
    {conn, result} = balance_request(conn)
    finish(conn, :balance, Response.from(result))
  end

  match _ do
    finish(conn, :send, Response.error(:method_not_allowed))
  end

  defp send_request(conn) do
    opts = opts(conn)

    with {:ok, conn, user, params} <- read_authenticated_form(conn, @send_keys),
         {:ok, dlr} <- DlrRequest.normalize(params),
         :ok <- DlrRequest.authorize(dlr, user),
         :ok <- require_dlr_available(opts, dlr),
         :ok <- require_publisher(opts.queue),
         {:ok, input} <- pipeline_input(user, params) do
      pipeline_opts =
        opts
        |> Map.take([:router, :queue, :id_fun, :dlr_store, :dlr_config, :dlr_clock])
        |> Map.put(:dlr_request, dlr)

      {conn, MtSubmitPipeline.submit(input, pipeline_opts)}
    else
      {:error, reason} -> {conn, {:error, reason}}
    end
  end

  defp rate_request(conn) do
    case read_authenticated_form(conn, @rate_keys) do
      {:ok, conn, user, params} ->
        {conn, quote_rate(opts(conn).router, user, params)}

      {:error, reason} ->
        {conn, {:error, reason}}
    end
  end

  defp balance_request(conn) do
    case read_authenticated_form(conn, @balance_keys) do
      {:ok, conn, user, _params} ->
        {conn, Queries.balance(Routing.snapshot(opts(conn).router), user.uid)}

      {:error, reason} ->
        {conn, {:error, reason}}
    end
  end

  defp read_authenticated_form(conn, allowed) do
    with :ok <- require_urlencoded(conn),
         {:ok, conn, params} <- parse_form(conn),
         :ok <- reject_query_credentials(conn),
         :ok <- reject_unknown(params, allowed),
         {:ok, user} <- authenticate(opts(conn).router, params) do
      {:ok, conn, user, params}
    end
  end

  defp quote_rate(router, user, params) do
    snapshot = Routing.snapshot(router)
    group = snapshot.groups[user.gid]

    with {:ok, to} <- require_param(params, "to", :missing_to),
         {:ok, routable} <-
           Routable.new(
             user: user,
             group: group,
             source: Map.get(params, "from", ""),
             destination: to,
             content: "",
             tags: []
           ) do
      Queries.quote(snapshot, routable)
    end
  end

  defp pipeline_input(user, params) do
    with {:ok, coding} <- parse_coding(Map.get(params, "coding")) do
      input =
        %{uid: user.uid, coding: coding}
        |> maybe_put(:to, params["to"])
        |> maybe_put(:from, params["from"])
        |> maybe_put(:content, params["content"])
        |> maybe_put(:hex_content, params["hex-content"])

      {:ok, input}
    end
  end

  defp authenticate(router, params) do
    username = Map.get(params, "username", "")
    password = Map.get(params, "password", "")

    if username == "" or password == "" do
      {:error, :invalid_credentials}
    else
      Routing.authenticate(router, username, password)
    end
  end

  defp require_urlencoded(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [value | _] ->
        if String.starts_with?(value, "application/x-www-form-urlencoded") do
          :ok
        else
          {:error, :unsupported_media_type}
        end

      _missing ->
        {:error, :unsupported_media_type}
    end
  end

  defp parse_form(conn) do
    case Plug.Conn.read_body(conn) do
      {:ok, body, conn} ->
        if malformed_urlencoded?(body) do
          {:error, :malformed_form}
        else
          {:ok, conn, URI.decode_query(body)}
        end

      _other ->
        {:error, :malformed_form}
    end
  end

  defp malformed_urlencoded?(body) do
    Regex.match?(~r/%(?:$|[^0-9A-Fa-f]|[0-9A-Fa-f](?:$|[^0-9A-Fa-f]))/, body)
  end

  defp reject_query_credentials(conn) do
    conn = Plug.Conn.fetch_query_params(conn)
    query = conn.query_params

    if Map.has_key?(query, "username") or Map.has_key?(query, "password") do
      {:error, :query_credential}
    else
      :ok
    end
  end

  defp reject_unknown(params, allowed) when is_list(allowed) do
    keys = MapSet.new(Map.keys(params))
    allowed = MapSet.new(allowed)

    if MapSet.subset?(keys, allowed) do
      :ok
    else
      {:error, :unknown_field}
    end
  end

  defp require_publisher(nil), do: {:error, :missing_publisher}
  defp require_publisher(_queue), do: :ok

  defp require_dlr_available(_opts, %DlrRequest{enabled: false}), do: :ok

  defp require_dlr_available(%{dlr_config: %{enabled: true}}, %DlrRequest{enabled: true}),
    do: :ok

  defp require_dlr_available(_opts, %DlrRequest{enabled: true}), do: {:error, :dlr_unavailable}

  defp require_param(params, key, reason) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, reason}
    end
  end

  defp maybe_put(map, _key, value) when value in [nil, ""], do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp parse_coding(value) when value in [nil, ""], do: {:ok, 0}

  defp parse_coding(value) when value in ["0", "1", "2", "3", "8"] do
    {:ok, String.to_integer(value)}
  end

  defp parse_coding(_value), do: {:error, :invalid_coding}

  defp finish(conn, endpoint, {status, body}) do
    Metrics.record(opts(conn).metrics, endpoint, status)

    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(status, body)
  end

  defp opts(conn), do: conn.private.http_api
end
