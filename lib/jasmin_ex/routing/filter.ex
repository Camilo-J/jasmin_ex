defmodule JasminEx.Routing.Filter do
  @moduledoc false

  alias JasminEx.Routing.Routable

  defmodule User do
    @moduledoc false
    @enforce_keys [:uid]
    defstruct [:uid]
    @type t :: %__MODULE__{uid: String.t()}
    @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_filter}
    def new(uid: uid) when is_binary(uid), do: {:ok, %__MODULE__{uid: uid}}
    def new(_attrs), do: {:error, :invalid_filter}
  end

  defmodule Group do
    @moduledoc false
    @enforce_keys [:gid]
    defstruct [:gid]
    @type t :: %__MODULE__{gid: String.t()}
    @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_filter}
    def new(gid: gid) when is_binary(gid), do: {:ok, %__MODULE__{gid: gid}}
    def new(_attrs), do: {:error, :invalid_filter}
  end

  defmodule Source do
    @moduledoc false
    @enforce_keys [:address]
    defstruct [:address]
    @type t :: %__MODULE__{address: String.t()}
    @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_filter}
    def new(address: address) when is_binary(address), do: {:ok, %__MODULE__{address: address}}
    def new(_attrs), do: {:error, :invalid_filter}
  end

  defmodule Destination do
    @moduledoc false
    @enforce_keys [:address]
    defstruct [:address]
    @type t :: %__MODULE__{address: String.t()}
    @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_filter}
    def new(address: address) when is_binary(address), do: {:ok, %__MODULE__{address: address}}
    def new(_attrs), do: {:error, :invalid_filter}
  end

  defmodule Content do
    @moduledoc false
    @enforce_keys [:body]
    defstruct [:body]
    @type t :: %__MODULE__{body: String.t()}
    @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_filter}
    def new(body: body) when is_binary(body), do: {:ok, %__MODULE__{body: body}}
    def new(_attrs), do: {:error, :invalid_filter}
  end

  defmodule TagsAll do
    @moduledoc false
    @enforce_keys [:tags]
    defstruct [:tags]
    @type t :: %__MODULE__{tags: [String.t()]}
    @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_filter}
    def new(tags: tags) when is_list(tags) do
      if Enum.all?(tags, &is_binary/1),
        do: {:ok, %__MODULE__{tags: tags}},
        else: {:error, :invalid_filter}
    end

    def new(_attrs), do: {:error, :invalid_filter}
  end

  @type t :: User.t() | Group.t() | Source.t() | Destination.t() | Content.t() | TagsAll.t()

  @spec valid?(term()) :: boolean()
  def valid?(%User{uid: uid}), do: ok?(User.new(uid: uid))
  def valid?(%Group{gid: gid}), do: ok?(Group.new(gid: gid))
  def valid?(%Source{address: address}), do: ok?(Source.new(address: address))
  def valid?(%Destination{address: address}), do: ok?(Destination.new(address: address))
  def valid?(%Content{body: body}), do: ok?(Content.new(body: body))
  def valid?(%TagsAll{tags: tags}), do: ok?(TagsAll.new(tags: tags))
  def valid?(_filter), do: false

  defp ok?({:ok, _value}), do: true
  defp ok?(_error), do: false

  @spec match?([term()], Routable.t()) :: boolean()
  def match?(filters, %Routable{} = routable) when is_list(filters) do
    Enum.all?(filters, &matches_one?(&1, routable))
  end

  defp matches_one?(%User{uid: uid}, %Routable{user: %{uid: uid}}), do: true
  defp matches_one?(%Group{gid: gid}, %Routable{group: %{gid: gid}}), do: true
  defp matches_one?(%Source{address: address}, %Routable{source: address}), do: true
  defp matches_one?(%Destination{address: address}, %Routable{destination: address}), do: true
  defp matches_one?(%Content{body: body}, %Routable{content: body}), do: true

  defp matches_one?(%TagsAll{tags: required}, %Routable{tags: present}) do
    MapSet.subset?(MapSet.new(required), MapSet.new(present))
  end

  defp matches_one?(_filter, _routable), do: false
end
