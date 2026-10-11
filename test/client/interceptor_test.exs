defmodule SimpleGrpc.Client.InterceptorTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Client
  alias SimpleGrpc.Context

  defmodule EchoServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/custom_prefix/custom_prefix_service.proto"

    def echo(%{message: msg}, _ctx) do
      {:ok, %{reply: "Hello, #{msg}!"}}
    end
  end

  defmodule StreamingServer do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(req, _ctx) do
      {:ok, %{result: "Echo: #{req.message}", sequence: req.sequence, is_final: true}}
    end

    def server_streaming_call(req, _ctx) do
      stream =
        Stream.map(1..req.sequence, fn i ->
          %{result: "#{req.message} ##{i}", sequence: i, is_final: i == req.sequence}
        end)

      {:ok, stream}
    end

    def client_streaming_call(req_stream, _ctx) do
      messages = Enum.map(req_stream, & &1.message)

      {:ok,
       %{
         result: "Received: #{Enum.join(messages, ", ")}",
         sequence: length(messages),
         is_final: true
       }}
    end

    def bidirectional_streaming_call(req_stream, _ctx) do
      resp_stream =
        Stream.map(req_stream, fn req ->
          %{result: "Bidi: #{req.message}", sequence: req.sequence, is_final: true}
        end)

      {:ok, resp_stream}
    end
  end

  # Module-based Interceptor
  defmodule PrefixMessageInterceptor do
    @behaviour SimpleGrpc.Interceptor

    @impl true
    def intercept(req, ctx, next, opts \\ []) do
      prefix = Keyword.get(opts, :prefix, "Dr. ")

      modified_req =
        if is_map(req) and Map.has_key?(req, :message),
          do: %{req | message: prefix <> req.message},
          else: req

      next.(modified_req, ctx)
    end
  end

  defmodule CustomHeaderInterceptor do
    @behaviour SimpleGrpc.Interceptor

    @impl true
    def intercept(req, ctx, next) do
      ctx = Context.put_header(ctx, "x-client-version", "2.0.0")
      next.(req, ctx)
    end
  end

  defmodule EchoClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/custom_prefix/custom_prefix_service.proto",
      interceptors: [{PrefixMessageInterceptor, [prefix: "Sir "]}]
  end

  defmodule PlainEchoClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/custom_prefix/custom_prefix_service.proto"
  end

  defmodule StreamingClient do
    use SimpleGrpc.Client,
      client_file: "priv/protos/streaming/streaming_service.proto"
  end

  setup do
    plug_state = SimpleGrpc.Plug.init(services: [EchoServer, StreamingServer])
    {:ok, plug_state: plug_state}
  end

  describe "client interceptor pipeline on unary calls" do
    test "module-level interceptor modifies outgoing request", %{plug_state: plug_state} do
      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      assert {:ok, resp} = EchoClient.echo(conn, %{message: "Lancelot"})
      assert resp.reply == "Hello, Sir Lancelot!"
    end

    test "per-call interceptor can override or compose with module interceptors", %{
      plug_state: plug_state
    } do
      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      suffix_interceptor = fn req, ctx, next ->
        req = %{req | message: req.message <> " of the Lake"}
        next.(req, ctx)
      end

      assert {:ok, resp} =
               EchoClient.echo(conn, %{message: "Lancelot"}, interceptors: [suffix_interceptor])

      assert resp.reply == "Hello, Sir Lancelot of the Lake!"
    end

    test "connection-level interceptors run before call interceptors", %{plug_state: plug_state} do
      order_agent = start_supervised!({Agent, fn -> [] end})

      conn_interceptor = fn req, ctx, next ->
        Agent.update(order_agent, &["conn_pre" | &1])
        result = next.(req, ctx)
        Agent.update(order_agent, &["conn_post" | &1])
        result
      end

      call_interceptor = fn req, ctx, next ->
        Agent.update(order_agent, &["call_pre" | &1])
        result = next.(req, ctx)
        Agent.update(order_agent, &["call_post" | &1])
        result
      end

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [conn_interceptor]
        )

      assert {:ok, _} =
               PlainEchoClient.echo(conn, %{message: "Alice"}, interceptors: [call_interceptor])

      history = Agent.get(order_agent, &Enum.reverse/1)
      assert history == ["conn_pre", "call_pre", "call_post", "conn_post"]
    end

    test "interceptor short-circuits call with cached result or error", %{plug_state: plug_state} do
      cache_interceptor = fn req, _ctx, _next ->
        if req.message == "CachedUser" do
          {:ok, %{reply: "Cached Response!"}}
        else
          {:error, 16, "Access denied"}
        end
      end

      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      # Short-circuit OK
      assert {:ok, resp} =
               PlainEchoClient.echo(conn, %{message: "CachedUser"},
                 interceptors: [cache_interceptor]
               )

      assert resp.reply == "Cached Response!"

      # Short-circuit ERROR
      assert {:error, 16, "Access denied"} =
               PlainEchoClient.echo(conn, %{message: "Stranger"},
                 interceptors: [cache_interceptor]
               )
    end

    test "interceptor mutates context headers", %{plug_state: plug_state} do
      header_recorder_interceptor = fn req, ctx, next ->
        ctx = Context.put_header(ctx, "x-request-id", "req-999")
        next.(req, ctx)
      end

      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          interceptors: [header_recorder_interceptor]
        )

      assert {:ok, resp} = PlainEchoClient.echo(conn, %{message: "Bob"})
      assert resp.reply == "Hello, Bob!"
    end

    test "dynamic invocation Client.call/4 supports interceptors", %{plug_state: plug_state} do
      {:ok, conn} =
        Client.connect(SimpleGrpc.Plug,
          adapter: :plug,
          plug_opts: plug_state,
          schema: "priv/protos/custom_prefix/custom_prefix_service.proto"
        )

      dynamic_interceptor = fn req, ctx, next ->
        req = %{req | message: "Dynamic " <> req.message}
        next.(req, ctx)
      end

      assert {:ok, resp} =
               Client.call(conn, "/custom_prefix.PrefixService/Echo", %{message: "World"},
                 interceptors: [dynamic_interceptor]
               )

      assert resp.reply == "Hello, Dynamic World!"
    end
  end

  describe "client interceptor pipeline on streaming calls" do
    test "server streaming with interceptor", %{plug_state: plug_state} do
      double_interceptor = fn req, ctx, next ->
        req = %{req | sequence: req.sequence * 2}
        next.(req, ctx)
      end

      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      assert {:ok, stream} =
               StreamingClient.server_streaming_call(
                 conn,
                 %{message: "Item", sequence: 2},
                 interceptors: [double_interceptor]
               )

      chunks = Enum.to_list(stream)
      assert length(chunks) == 4
      assert Enum.map(chunks, & &1.sequence) == [1, 2, 3, 4]
    end

    test "client streaming with interceptor", %{plug_state: plug_state} do
      transform_stream_interceptor = fn req_stream, ctx, next ->
        mapped =
          Stream.map(req_stream, fn item ->
            %{item | message: String.upcase(item.message)}
          end)

        next.(mapped, ctx)
      end

      {:ok, conn} = Client.connect(SimpleGrpc.Plug, adapter: :plug, plug_opts: plug_state)

      requests = [%{message: "one"}, %{message: "two"}, %{message: "three"}]

      assert {:ok, resp} =
               StreamingClient.client_streaming_call(conn, requests,
                 interceptors: [transform_stream_interceptor]
               )

      assert resp.result == "Received: ONE, TWO, THREE"
      assert resp.sequence == 3
    end
  end
end
