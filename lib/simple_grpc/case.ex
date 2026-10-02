defmodule SimpleGrpc.Case do
  @moduledoc """
  ExUnit test case template for lightning-fast, zero-TCP in-memory testing of gRPC servers and Plugs.

  ## Usage

  Define a test module using `SimpleGrpc.Case`:

      defmodule MyServerTest do
        use SimpleGrpc.Case, plug: MyGrpcPlug

        test "invokes Echo RPC in-memory", %{conn: conn} do
          req = %{message: "Hello"}
          {:ok, resp_bytes, _headers, _trailers} =
            SimpleGrpc.Client.send_unary(conn, "/echo.EchoService/Echo", req_bytes)

          assert resp_bytes != nil
        end
      end

  You can also override or pass the Plug per-test via ExUnit `@tag`:

      @tag plug: AnotherCustomPlug
      test "tests isolated plug", %{conn: conn} do
        ...
      end
  """

  use ExUnit.CaseTemplate

  using opts do
    default_target = opts[:plug] || opts[:server] || SimpleGrpc.Plug
    default_plug_opts = opts[:plug_opts] || []

    quote do
      import SimpleGrpc.Case

      setup tags do
        plug_target = tags[:plug] || tags[:server] || unquote(default_target)
        plug_opts = tags[:plug_opts] || unquote(default_plug_opts)

        {:ok, conn} =
          SimpleGrpc.Client.connect(plug_target, adapter: :plug, plug_opts: plug_opts)

        %{conn: conn}
      end
    end
  end

  @doc """
  Helper to make a unary gRPC call on an in-memory client connection.
  """
  def grpc_call(conn, path, payload, opts \\ []) do
    SimpleGrpc.Client.send_unary(conn, path, payload, opts)
  end

  @doc """
  Helper to make a streaming gRPC call on an in-memory client connection.
  """
  def grpc_stream(conn, path, payload_or_stream, opts \\ []) do
    SimpleGrpc.Client.send_stream(conn, path, payload_or_stream, opts)
  end
end
