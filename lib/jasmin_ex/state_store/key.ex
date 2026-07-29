defmodule JasminEx.StateStore.Key do
  @moduledoc false
  @max_component_size 65_535

  def encode({prefix, version}, key)
      when is_binary(prefix) and is_binary(version) and is_binary(key) and
             byte_size(prefix) <= @max_component_size and
             byte_size(version) <= @max_component_size and
             byte_size(key) <= @max_component_size do
    <<"JXSS", 1, byte_size(prefix)::32, prefix::binary, byte_size(version)::16, version::binary,
      byte_size(key)::64, key::binary>>
  end

  def encode({prefix, version}, _key) when not is_binary(prefix) or not is_binary(version),
    do: {:error, {:invalid_argument, :key_namespace}}

  def encode(_namespace, _key), do: {:error, {:invalid_argument, :key}}
end
