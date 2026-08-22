defmodule JasminEx.Routing.Routable do
  @moduledoc false

  alias JasminEx.Routing.Group
  alias JasminEx.Routing.User

  @enforce_keys [:user, :group, :source, :destination, :content, :tags]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          user: User.t(),
          group: Group.t(),
          source: String.t(),
          destination: String.t(),
          content: String.t(),
          tags: [String.t()]
        }

  @type error ::
          :invalid_user
          | :invalid_group
          | :invalid_source
          | :invalid_destination
          | :invalid_content
          | :invalid_tags

  @spec new(keyword()) :: {:ok, t()} | {:error, error()}
  def new(attrs) when is_list(attrs) do
    with {:ok, user} <- require_user(Keyword.get(attrs, :user)),
         {:ok, group} <- require_group(Keyword.get(attrs, :group)),
         {:ok, source} <- require_binary(Keyword.get(attrs, :source), :invalid_source),
         {:ok, destination} <-
           require_binary(Keyword.get(attrs, :destination), :invalid_destination),
         {:ok, content} <- require_binary(Keyword.get(attrs, :content), :invalid_content),
         {:ok, tags} <- require_tags(Keyword.get(attrs, :tags, [])) do
      {:ok,
       struct!(__MODULE__,
         user: user,
         group: group,
         source: source,
         destination: destination,
         content: content,
         tags: tags
       )}
    end
  end

  def new(_attrs), do: {:error, :invalid_user}

  defp require_user(%User{} = user), do: {:ok, user}
  defp require_user(_user), do: {:error, :invalid_user}
  defp require_group(%Group{} = group), do: {:ok, group}
  defp require_group(_group), do: {:error, :invalid_group}
  defp require_binary(value, _reason) when is_binary(value), do: {:ok, value}
  defp require_binary(_value, reason), do: {:error, reason}

  defp require_tags(tags) when is_list(tags) do
    if Enum.all?(tags, &is_binary/1), do: {:ok, tags}, else: {:error, :invalid_tags}
  end

  defp require_tags(_tags), do: {:error, :invalid_tags}
end
