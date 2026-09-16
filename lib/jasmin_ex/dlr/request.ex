defmodule JasminEx.Dlr.Request do
  @moduledoc false

  alias JasminEx.Routing.User

  defstruct enabled: false,
            level: nil,
            method: nil,
            url: nil,
            request_receipt: false,
            register_callback: false

  @type t :: %__MODULE__{
          enabled: boolean(),
          level: 1 | 2 | 3 | nil,
          method: String.t() | nil,
          url: String.t() | nil,
          request_receipt: boolean(),
          register_callback: boolean()
        }

  @spec normalize(map()) :: {:ok, t()} | {:error, atom()}
  def normalize(params) when is_map(params) do
    with :ok <- reject_unknown_expiry(params),
         {:ok, method} <- parse_method(Map.get(params, "dlr-method")),
         {:ok, url} <- parse_url(Map.get(params, "dlr-url")),
         {:ok, level} <- parse_level(Map.get(params, "dlr-level")),
         {:ok, enabled} <- enabled?(params, url, level) do
      {:ok, build(enabled, level, method, url)}
    end
  end

  def normalize(_params), do: {:error, :invalid_dlr}

  @spec authorize(t(), User.t()) :: :ok | {:error, :user_disabled | :dlr_forbidden}
  def authorize(%__MODULE__{}, %User{enabled: false}), do: {:error, :user_disabled}

  def authorize(%__MODULE__{enabled: true}, %User{} = user) do
    if user.set_dlr_level and user.http_set_dlr_method, do: :ok, else: {:error, :dlr_forbidden}
  end

  def authorize(%__MODULE__{method: method}, %User{} = user) when not is_nil(method) do
    if user.http_set_dlr_method, do: :ok, else: {:error, :dlr_forbidden}
  end

  def authorize(%__MODULE__{}, %User{}), do: :ok

  defp reject_unknown_expiry(params) do
    if Map.has_key?(params, "dlr-expiry"), do: {:error, :unknown_field}, else: :ok
  end

  defp enabled?(params, url, level) do
    forced? = not is_nil(url) or not is_nil(level)

    case Map.get(params, "dlr") do
      nil when forced? -> {:ok, true}
      nil -> {:ok, false}
      "yes" -> {:ok, true}
      "no" when forced? -> {:ok, true}
      "no" -> {:ok, false}
      _other -> {:error, :invalid_dlr}
    end
  end

  defp parse_method(nil), do: {:ok, nil}

  defp parse_method(method) when is_binary(method) do
    case String.upcase(method) do
      normalized when normalized in ["GET", "POST"] -> {:ok, normalized}
      _other -> {:error, :invalid_dlr_method}
    end
  end

  defp parse_method(_method), do: {:error, :invalid_dlr_method}

  defp parse_url(nil), do: {:ok, nil}

  defp parse_url(url) when is_binary(url) do
    uri = URI.parse(url)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.host != "" do
      {:ok, url}
    else
      {:error, :invalid_dlr_url}
    end
  end

  defp parse_url(_url), do: {:error, :invalid_dlr_url}

  defp parse_level(nil), do: {:ok, nil}
  defp parse_level("1"), do: {:ok, 1}
  defp parse_level("2"), do: {:ok, 2}
  defp parse_level("3"), do: {:ok, 3}
  defp parse_level(1), do: {:ok, 1}
  defp parse_level(2), do: {:ok, 2}
  defp parse_level(3), do: {:ok, 3}
  defp parse_level(_level), do: {:error, :invalid_dlr_level}

  defp build(false, _level, method, _url) do
    %__MODULE__{enabled: false, method: method}
  end

  defp build(true, level, method, url) do
    %__MODULE__{
      enabled: true,
      level: level || 1,
      method: method || "POST",
      url: url,
      request_receipt: true,
      register_callback: not is_nil(url)
    }
  end
end
