defmodule JasminEx.Routing.FileOps do
  @moduledoc false
  def mkdir_p(path), do: File.mkdir_p(path)
  def write(path, data), do: File.write(path, data)
  def rename(from, to), do: File.rename(from, to)
  def chmod(path, mode), do: File.chmod(path, mode)
  def read(path), do: File.read(path)

  def fsync(path) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      result = :file.sync(fd)
      _ = :file.close(fd)
      result
    end
  end
end
