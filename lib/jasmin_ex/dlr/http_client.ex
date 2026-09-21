defmodule JasminEx.Dlr.HttpClient do
  @moduledoc false

  @type request :: %{
          method: binary(),
          url: binary(),
          headers: [{binary(), binary()}],
          body: binary()
        }
  @type response :: {:ok, non_neg_integer(), binary()} | {:error, term()}

  @callback request(term(), request()) :: response()
end
