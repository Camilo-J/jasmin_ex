defmodule JasminEx.Routing do
  @moduledoc """
  Public context for MT routing identity, eligibility, and resolution.
  """

  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Credential
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.Route
  alias JasminEx.Routing.Router
  alias JasminEx.Routing.RouteTable
  alias JasminEx.Routing.State
  alias JasminEx.Routing.Telemetry
  alias JasminEx.Routing.User

  defdelegate snapshot(server), to: Router
  defdelegate put_group(server, attrs), to: Router
  defdelegate put_user(server, attrs), to: Router
  defdelegate put_route(server, attrs), to: Router
  defdelegate delete_group(server, gid), to: Router

  @doc """
  Admit one bill through the live Router.

  The first admission persists the reservation before publication. An exact
  duplicate is a no-op. The same `bill_id` with a different economic
  fingerprint returns `{:error, :billing_conflict}` and leaves state unchanged.
  """
  @spec admit(GenServer.server(), term()) ::
          {:ok, JasminEx.Billing.Reservation.t()}
          | {:ok, :duplicate, JasminEx.Billing.Bill.t(), JasminEx.Billing.Fingerprint.t()}
          | {:error, atom()}
  defdelegate admit(server, admission), to: Router

  @spec settle(GenServer.server(), term()) ::
          {:ok, JasminEx.Billing.Tombstone.t()}
          | {:ok, :duplicate | :late_ignored}
          | {:error, atom()}
  defdelegate settle(server, settlement), to: Router

  @spec expire_due(GenServer.server()) :: {:ok, non_neg_integer()} | {:error, atom()}
  defdelegate expire_due(server), to: Router

  @doc """
  Set a user's integer balance in minor units and persist before publication.

  `nil` means unlimited. An unchanged value is a no-op. Invalid input fails
  typed without a write or revision change.
  """
  @spec set_balance(GenServer.server(), term(), term()) :: {:ok, User.t()} | {:error, atom()}
  defdelegate set_balance(server, uid, amount), to: Router

  @spec set_quota(GenServer.server(), term(), term()) :: {:ok, User.t()} | {:error, atom()}
  defdelegate set_quota(server, uid, amount), to: Router
  defdelegate set_smpp_secret(server, uid, secret), to: Router
  defdelegate set_max_bindings(server, uid, limit), to: Router

  @spec set_rate(GenServer.server(), term(), term()) :: {:ok, Route.t()} | {:error, atom()}
  defdelegate set_rate(server, order, rate), to: Router

  @spec authenticate(GenServer.server(), String.t(), term()) ::
          {:ok, User.t()} | {:error, :invalid_credentials | :user_disabled | :group_disabled}
  def authenticate(server, username, secret) do
    authenticate_snapshot(Router.snapshot(server), username, secret)
  end

  def authenticate_smpp(server, system_id, secret) do
    authenticate_smpp_snapshot(Router.snapshot(server), system_id, secret)
  end

  @spec authenticate_snapshot(State.t(), String.t(), term()) ::
          {:ok, User.t()} | {:error, :invalid_credentials | :user_disabled | :group_disabled}
  def authenticate_snapshot(%State{} = state, username, secret) do
    result =
      case find_user(state.users, username) do
        nil -> {:error, :invalid_credentials}
        user -> eligibility(state, user, secret)
      end

    Telemetry.emit([:auth], %{}, %{result: class(result)})
    result
  end

  def authenticate_smpp_snapshot(%State{} = state, system_id, secret) do
    case find_user(state.users, system_id) do
      nil -> {:error, :invalid_credentials}
      user -> smpp_eligibility(state, user, secret)
    end
  end

  defp eligibility(state, user, secret) do
    cond do
      not Credential.verify(user.credential, secret) -> {:error, :invalid_credentials}
      not user.enabled -> {:error, :user_disabled}
      not group_enabled?(state, user.gid) -> {:error, :group_disabled}
      true -> {:ok, user}
    end
  end

  defp smpp_eligibility(state, user, secret) do
    cond do
      is_nil(user.smpp_credential) -> {:error, :invalid_credentials}
      not Credential.verify(user.smpp_credential, secret) -> {:error, :invalid_credentials}
      not user.enabled -> {:error, :user_disabled}
      not group_enabled?(state, user.gid) -> {:error, :group_disabled}
      true -> {:ok, user}
    end
  end

  @spec resolve(GenServer.server(), Routable.t()) :: {:ok, ConnectorRef.t()} | {:error, :no_route}
  def resolve(server, %Routable{} = routable) do
    resolve_snapshot(Router.snapshot(server), routable)
  end

  @spec resolve_snapshot(State.t(), Routable.t()) :: {:ok, ConnectorRef.t()} | {:error, :no_route}
  def resolve_snapshot(%State{routes: table}, %Routable{} = routable) do
    started = System.monotonic_time()

    {result, order} =
      case RouteTable.winning_route(table, routable) do
        %Route{connector: connector, order: order} -> {{:ok, connector}, order}
        nil -> {{:error, :no_route}, nil}
      end

    Telemetry.emit([:resolve], %{duration: System.monotonic_time() - started}, %{
      result: class(result),
      order: order
    })

    result
  end

  defp class({:ok, _value}), do: :ok
  defp class({:error, reason}), do: reason

  defp group_enabled?(%{groups: groups}, gid), do: match?(%Group{enabled: true}, groups[gid])

  defp find_user(users, username) do
    Enum.find_value(users, fn {_uid, user} -> user.username == username && user end)
  end
end
