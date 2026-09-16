defmodule JasminEx.Dlr.RequestTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Request
  alias JasminEx.Routing.Group
  alias JasminEx.Routing.User

  describe "normalize/1" do
    test "no DLR fields leaves DLR disabled" do
      assert {:ok, request} = Request.normalize(%{"to" => "21200000", "content" => "hi"})
      assert request.enabled == false
      assert request.level == nil
      assert request.method == nil
      assert request.url == nil
      assert request.request_receipt == false
      assert request.register_callback == false
    end

    test "dlr=yes defaults to level 1 POST" do
      assert {:ok, request} =
               Request.normalize(%{"dlr" => "yes", "dlr-url" => "http://example.com/dlr"})

      assert request.enabled == true
      assert request.level == 1
      assert request.method == "POST"
      assert request.url == "http://example.com/dlr"
      assert request.request_receipt == true
      assert request.register_callback == true
    end

    test "dlr-url or dlr-level forces enable including with dlr=no" do
      assert {:ok, by_url} =
               Request.normalize(%{
                 "dlr" => "no",
                 "dlr-url" => "https://example.com/dlr"
               })

      assert by_url.enabled == true
      assert by_url.level == 1
      assert by_url.method == "POST"
      assert by_url.register_callback == true

      assert {:ok, by_level} = Request.normalize(%{"dlr" => "no", "dlr-level" => "2"})
      assert by_level.enabled == true
      assert by_level.level == 2
      assert by_level.method == "POST"
      assert by_level.request_receipt == true
      assert by_level.register_callback == false
      assert by_level.url == nil
    end

    test "method-only does not enable DLR" do
      assert {:ok, request} = Request.normalize(%{"dlr-method" => "GET"})
      assert request.enabled == false
      assert request.method == "GET"
      assert request.request_receipt == false
      assert request.register_callback == false
    end

    test "enabled without URL requests a receipt but does not register a callback" do
      assert {:ok, request} = Request.normalize(%{"dlr" => "yes"})
      assert request.enabled == true
      assert request.level == 1
      assert request.method == "POST"
      assert request.url == nil
      assert request.request_receipt == true
      assert request.register_callback == false
    end

    test "invalid dlr, level, method, URL, and per-request expiry are rejected" do
      assert {:error, :invalid_dlr} = Request.normalize(%{"dlr" => "maybe"})
      assert {:error, :invalid_dlr_level} = Request.normalize(%{"dlr-level" => "4"})
      assert {:error, :invalid_dlr_level} = Request.normalize(%{"dlr-level" => "0"})
      assert {:error, :invalid_dlr_method} = Request.normalize(%{"dlr-method" => "PUT"})

      assert {:error, :invalid_dlr_url} =
               Request.normalize(%{"dlr-url" => "ftp://example.com/dlr"})

      assert {:error, :unknown_field} = Request.normalize(%{"dlr-expiry" => "3600"})
    end

    test "dlr-method get is stored as GET" do
      assert {:ok, request} =
               Request.normalize(%{
                 "dlr" => "yes",
                 "dlr-url" => "http://example.com/dlr",
                 "dlr-method" => "get"
               })

      assert request.method == "GET"
    end
  end

  describe "authorize/2" do
    test "disabled user is rejected" do
      user = user!(enabled: false)
      assert {:ok, request} = Request.normalize(%{"dlr" => "yes"})
      assert {:error, :user_disabled} = Request.authorize(request, user)
    end

    test "permissions false with omitted level and method on enabled DLR is dlr_forbidden" do
      user = user!(set_dlr_level: false, http_set_dlr_method: false)

      assert {:ok, request} =
               Request.normalize(%{"dlr" => "yes", "dlr-url" => "http://example.com/dlr"})

      assert {:error, :dlr_forbidden} = Request.authorize(request, user)
    end

    test "permissions false with explicit level and method is dlr_forbidden" do
      user = user!(set_dlr_level: false, http_set_dlr_method: false)

      assert {:ok, request} =
               Request.normalize(%{
                 "dlr" => "yes",
                 "dlr-level" => "2",
                 "dlr-method" => "GET",
                 "dlr-url" => "http://example.com/dlr"
               })

      assert {:error, :dlr_forbidden} = Request.authorize(request, user)
    end

    test "method-only still requires http_set_dlr_method" do
      user = user!(http_set_dlr_method: false)
      assert {:ok, request} = Request.normalize(%{"dlr-method" => "POST"})
      assert {:error, :dlr_forbidden} = Request.authorize(request, user)

      allowed = user!(http_set_dlr_method: true)
      assert :ok = Request.authorize(request, allowed)
    end

    test "permissions true allow defaulted level and method" do
      user = user!(set_dlr_level: true, http_set_dlr_method: true)

      assert {:ok, request} =
               Request.normalize(%{"dlr" => "yes", "dlr-url" => "http://example.com/dlr"})

      assert :ok = Request.authorize(request, user)
    end

    test "level-only denial still forbids when method permission is true" do
      user = user!(set_dlr_level: false, http_set_dlr_method: true)
      assert {:ok, request} = Request.normalize(%{"dlr" => "yes"})
      assert {:error, :dlr_forbidden} = Request.authorize(request, user)
    end
  end

  defp user!(attrs) do
    {:ok, group} = Group.new(gid: "ops")

    {:ok, user} =
      User.new(
        [
          uid: "u1",
          username: "alice",
          secret: "s3cret",
          group: group
        ] ++ Keyword.take(attrs, [:enabled, :set_dlr_level, :http_set_dlr_method])
      )

    user
  end
end
