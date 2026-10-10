defmodule SimpleGrpc.ServerInterceptorTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Client
  alias SimpleGrpc.Context

  defmodule AuthValidatorInterceptor do
    @behaviour SimpleGrpc.Interceptor

    @impl true
    def intercept(req, ctx, next) do
      auth_header = Context.get_header(ctx, "authorization")

      if auth_header == "Bearer secret-token-123" do
        ctx = Context.put_private(ctx, :current_user, %{name: "Admin", role: :admin})
        next.(req, ctx)
      else
        {:error, 16, "Unauthenticated: invalid or missing bearer token"}
      end
    end
  end

  defmodule UppercaseRequestInterceptor do
    @behaviour SimpleGrpc.Interceptor

    @impl true
    def intercept(req, ctx, next) do
      modified_req =
        if is_map(req) and Map.has_key?(req, :message) do
          %{req | message: String.upcase(req.message)}
        else
          req
        end

      next.(modified_req, ctx)
    end
  end

  defmodule ResponseTransformInterceptor do
    @behaviour SimpleGrpc.Interceptor

    @impl true
    def intercept(req, ctx, next) do
      case next.(req, ctx) do
        {:ok, resp} ->
          {:ok, %{resp | reply: resp.reply <> " [INTERCEPTED]"}}

        other ->
          other
      end
    end
  end

  defmodule SecuredEchoServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/custom_prefix/custom_prefix_service.proto",
      interceptors: [UppercaseRequestInterceptor]

    def echo(req, ctx) do
      user = Context.get_private(ctx, :current_user)
      username = if user, do: user.name, else: "Anonymous"
      {:ok, %{reply: "Hello, #{req.message}! (handled by #{username})"}}
    end
  end

  defmodule PlainEchoClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/custom_prefix/custom_prefix_service.proto"
  end

  describe "server interceptors execution" do
    test "service-level interceptor modifies request payload and passes ctx to handler" do
      plug_state =
        SimpleGrpc.Plug.init(
          services: [SecuredEchoServer],
          interceptors: [AuthValidatorInterceptor]
        )

      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      # 1. Without auth header -> fails with 16 (Unauthenticated)
      assert {:error, 16, msg} = PlainEchoClient.echo(conn, %{message: "alice"})
      assert msg =~ "Unauthenticated"

      # 2. With auth header -> passes auth, uppercase interceptor runs, handler receives ctx with user
      assert {:ok, resp} =
               PlainEchoClient.echo(conn, %{message: "alice"},
                 headers: [{"authorization", "Bearer secret-token-123"}]
               )

      assert resp.reply == "Hello, ALICE! (handled by Admin)"
    end

    test "plug-level interceptor post-processes handler response" do
      plug_state =
        SimpleGrpc.Plug.init(
          services: [SecuredEchoServer],
          interceptors: [
            fn req, ctx, next ->
              ctx = Context.put_header(ctx, "authorization", "Bearer secret-token-123")
              next.(req, ctx)
            end,
            AuthValidatorInterceptor,
            ResponseTransformInterceptor
          ]
        )

      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      assert {:ok, resp} = PlainEchoClient.echo(conn, %{message: "bob"})
      assert resp.reply == "Hello, BOB! (handled by Admin) [INTERCEPTED]"
    end
  end
end
