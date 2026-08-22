defmodule JasminEx.Routing do
  @moduledoc """
  Public context for MT routing identity, eligibility, and resolution.
  """

  alias JasminEx.Routing.Router

  defdelegate snapshot(server), to: Router
  defdelegate put_group(server, attrs), to: Router
  defdelegate put_user(server, attrs), to: Router
  defdelegate put_route(server, attrs), to: Router
  defdelegate delete_group(server, gid), to: Router
end
