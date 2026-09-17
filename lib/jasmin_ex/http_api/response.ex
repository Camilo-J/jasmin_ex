defmodule JasminEx.HttpApi.Response do
  @moduledoc false

  @statuses %{
    method_not_allowed: 405,
    unknown_field: 400,
    query_credential: 400,
    malformed_form: 400,
    malformed_hex: 400,
    missing_to: 400,
    missing_from: 400,
    missing_content: 400,
    ambiguous_content: 400,
    invalid_coding: 400,
    invalid_dlr: 400,
    invalid_dlr_level: 400,
    invalid_dlr_method: 400,
    invalid_dlr_url: 400,
    dlr_forbidden: 400,
    invalid_credentials: 401,
    user_disabled: 401,
    group_disabled: 401,
    insufficient_balance: 402,
    insufficient_quota: 402,
    no_route: 404,
    unsupported_media_type: 415,
    missing_publisher: 503,
    dlr_unavailable: 503,
    non_ok: 503,
    internal: 500
  }

  def ok(body) when is_binary(body), do: {200, trailing_newline(body)}
  def ok(amount) when is_integer(amount), do: {200, "#{amount}\n"}
  def ok(:unlimited), do: {200, "unlimited\n"}

  def error(reason) when is_atom(reason) do
    case Map.fetch(@statuses, reason) do
      {:ok, status} -> {status, "error:#{reason}\n"}
      :error -> {500, "error:internal\n"}
    end
  end

  def from({:ok, value}), do: ok(value)
  def from({:error, {:validate, reason}}), do: error(reason)
  def from({:error, {:route, reason}}), do: error(reason)
  def from({:error, {:bill, reason}}), do: error(reason)
  def from({:error, {:dispatch, _reason}}), do: error(:non_ok)
  def from({:error, reason}) when is_atom(reason), do: error(reason)
  def from(_other), do: error(:internal)

  defp trailing_newline(body) do
    if String.ends_with?(body, "\n"), do: body, else: body <> "\n"
  end
end
