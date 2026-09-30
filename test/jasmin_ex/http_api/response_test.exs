defmodule JasminEx.HttpApi.ResponseTest do
  use ExUnit.Case, async: true

  alias JasminEx.HttpApi.Response

  test "invalid_content and message_too_long are HTTP 400 with the existing error contract" do
    assert Response.error(:invalid_content) == {400, "error:invalid_content\n"}
    assert Response.error(:message_too_long) == {400, "error:message_too_long\n"}

    assert Response.from({:error, {:validate, :invalid_content}}) ==
             {400, "error:invalid_content\n"}

    assert Response.from({:error, {:validate, :message_too_long}}) ==
             {400, "error:message_too_long\n"}
  end

  test "unknown atoms stay internal 500 and do not leak a fallback reason" do
    assert Response.error(:not_a_real_reason) == {500, "error:internal\n"}
  end
end
