defmodule SimpleGrpc.Client do
  @moduledoc """
  Pure-Elixir gRPC client supporting strongly-typed code generation, dynamic reflection-based invocations, HTTP/2 network transport, and direct in-memory testing.

  ## Features
  - **Typed Client DSL**: Generate compile-time client modules with autocomplete, typed request/response structs, and bang variants using `use SimpleGrpc.Client`.
  - **Dynamic Invocations**: Call any gRPC method dynamically with `SimpleGrpc.Client.call/4` using server reflection or a runtime `%SimpleGrpc.Schema{}`.
  - **Transport Adapters**:
    - `SimpleGrpc.Client.Adapters.Mint` (`:mint`) - Pure-Elixir HTTP/2 transport over TCP/TLS with connection pooling and stream multiplexing.
    - `SimpleGrpc.Client.Adapters.Plug` (`:plug`) - Zero-overhead, in-memory transport dispatching directly to `SimpleGrpc.Plug` without socket/network overhead.
  - **Streaming Support**: Full support for server streaming (as lazy `Stream.resource` Elixir streams), client streaming (from Enumerable/Stream inputs), and bidirectional streaming.
  - **Observability**: Built-in Telemetry events (`[:simple_grpc, :client, :start | :stop | :exception]`).

  ## Typed Client Usage
  Define a client module by passing a `.proto` file path or a `%SimpleGrpc.Schema{}` struct:

      defmodule MyApp.GreeterClient do
        use SimpleGrpc.Client,
          client_file: "priv/protos/greeter.proto"
      end

  ### Making Calls
      # 1. Connect to server
      {:ok, conn} = SimpleGrpc.Client.connect("localhost:50051", adapter: :mint)

      # 2. Invoke unary RPC
      {:ok, response} = MyApp.GreeterClient.say_hello(conn, %{name: "Alice"})
      # Or raising on error:
      response = MyApp.GreeterClient.say_hello!(conn, %{name: "Alice"})

      # 3. Server streaming RPC (returns a lazy Elixir Stream)
      {:ok, stream} = MyApp.GreeterClient.lots_of_replies(conn, %{name: "Alice"})
      Enum.each(stream, fn reply -> IO.puts(reply.message) end)

      # 4. Client streaming RPC (accepts a List or Stream)
      requests = [%{name: "Alice"}, %{name: "Bob"}]
      {:ok, summary} = MyApp.GreeterClient.lots_of_greetings(conn, requests)

      # 5. Bidirectional streaming RPC
      input_stream = Stream.map(1..10, fn i -> %{message: "Msg \#{i}"} end)
      {:ok, output_stream} = MyApp.GreeterClient.bidi_stream(conn, input_stream)

  ## In-Memory Testing (Plug Adapter)
  You can test your gRPC services directly in-memory without starting an HTTP/2 TCP listener:

      {:ok, conn} = SimpleGrpc.Client.connect(MyGrpcPlug, adapter: :plug)
      {:ok, response} = MyApp.GreeterClient.say_hello(conn, %{name: "Test"})

  ## Dynamic Reflection Client
  Connect to any reflection-enabled gRPC server without local `.proto` files:

      {:ok, conn} = SimpleGrpc.Client.connect("localhost:50051", reflection: true)
      {:ok, response} = SimpleGrpc.Client.call(conn, "greeter.Greeter/SayHello", %{name: "Dynamic"})

  ## Connection Options (`connect/2`)
  - `:adapter` - `:mint` (default for network targets) or `:plug` (default for Plug modules).
  - `:reflection` - (boolean) Query server reflection upon connecting to populate schema metadata.
  - `:schema` - Pre-loaded `%SimpleGrpc.Schema{}` struct or proto file path.
  - `:scheme` - `:http` (default) or `:https`.
  - `:timeout` - Request timeout in milliseconds (default: `5000`).
  - `:headers` - Additional default request headers / metadata (e.g. `[{"authorization", "Bearer token"}]`).
  - `:plug_opts` - Options passed to `SimpleGrpc.Plug.init/1` when using `:plug` adapter.
  """

  alias SimpleGrpc.Client.Conn
  alias SimpleGrpc.Client.DSL

  defmacro __using__(opts) do
    DSL.generate(opts, __CALLER__.module)
  end

  @doc """
  Connects to a gRPC server target using the specified or auto-detected adapter.

  ## Options
  - `:adapter` - Transport adapter module (`SimpleGrpc.Client.Adapters.Mint` or `SimpleGrpc.Client.Adapters.Plug`, or shorthand `:mint` / `:plug`).
  - `:reflection` - (boolean, default: `false`) when `true`, automatically queries the server via gRPC reflection and populates the schema.
  - `:schema` - Pre-loaded `%SimpleGrpc.Schema{}` struct.
  - `:scheme` - `:http` (default) or `:https`.
  - `:timeout` - Request timeout in milliseconds (default: `5000`).
  - `:headers` - Additional default request headers / metadata.
  """
  def connect(target, opts \\ []) do
    adapter = resolve_adapter(target, opts)

    case adapter.connect(target, opts) do
      {:ok, adapter_state} ->
        raw_schema = Keyword.get(opts, :schema)

        schema =
          cond do
            is_binary(raw_schema) ->
              case SimpleGrpc.Schema.from_file(raw_schema) do
                {:ok, s} -> s
                _ -> raw_schema
              end

            true ->
              raw_schema
          end

        conn = %Conn{
          adapter: adapter,
          state: adapter_state,
          target: target,
          opts: opts,
          schema: schema
        }

        if Keyword.get(opts, :reflection, false) do
          case SimpleGrpc.Client.Reflection.fetch_schema(conn, opts) do
            {:ok, schema} ->
              {:ok, %{conn | schema: schema}}

            {:error, reason} ->
              {:error, {:reflection_failed, reason}}
          end
        else
          {:ok, conn}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Closes the client connection.
  """
  def close(%Conn{adapter: adapter, state: state}) do
    adapter.close(state)
  end

  @doc """
  Sends an HTTP/2 PING frame on the connection to verify liveness and measure round-trip latency.
  Returns `{:ok, latency_us}` in microseconds, or `{:error, reason}`.

  ## Options
  - `:timeout` - Ping timeout in milliseconds (default: `5000`).
  - `:payload` - 8-byte binary payload (default: 8 random bytes).
  """
  def ping(%Conn{adapter: adapter, state: state}, opts \\ []) do
    if function_exported?(adapter, :ping, 2) do
      adapter.ping(state, opts)
    else
      {:error, :not_supported}
    end
  end

  @doc """
  Invokes an RPC dynamically using reflection or an attached runtime schema.

  ## Arguments
  - `conn` - `%SimpleGrpc.Client.Conn{}` connection struct.
  - `rpc_or_path` - Canonical path (e.g. `"/streaming.StreamingService/UnaryCall"` or `"streaming.StreamingService/UnaryCall"`),
    or `{ServiceName, :method_name}` tuple.
  - `payload` - Request message payload (map, struct, keyword list, or stream of payloads).
  - `opts` - Optional keyword list with `:headers`, `:timeout`, `:schema`, etc.

  ## Examples
      {:ok, client} = SimpleGrpc.Client.connect("localhost:50051", reflection: true)
      {:ok, response} = SimpleGrpc.Client.call(client, "streaming.StreamingService/UnaryCall", %{message: "Hi"})
  """
  def call(conn, rpc_or_path, payload, opts \\ [])

  def call(%Conn{} = conn, rpc_or_path, payload, opts) do
    with {:ok, schema} <- resolve_call_schema(conn, rpc_or_path, opts),
         {:ok, rpc} <- resolve_call_rpc(schema, rpc_or_path) do
      invoke_dynamic_rpc(conn, rpc, payload, schema, opts)
    end
  end

  @doc """
  Invokes an RPC dynamically, raising on error.
  """
  def call!(%Conn{} = conn, rpc_or_path, payload, opts \\ []) do
    case call(conn, rpc_or_path, payload, opts) do
      {:ok, resp} ->
        resp

      {:error, status, msg} ->
        raise RuntimeError, "gRPC error #{status}: #{msg}"
    end
  end

  @doc """
  Returns the `%SimpleGrpc.Schema{}` struct attached to the connection, if any.
  """
  def schema(%Conn{schema: schema}), do: schema

  @doc """
  Sends a unary RPC request to the given canonical gRPC path with an encoded binary payload.
  """
  def send_unary(%Conn{adapter: adapter, state: state} = conn, path, payload, opts \\ []) do
    merged_opts = SimpleGrpc.Client.Headers.merge_opts(conn, opts)
    adapter.send_unary(state, path, payload, merged_opts)
  end

  @doc """
  Sends a streaming RPC request to the given canonical gRPC path.
  """
  def send_stream(%Conn{adapter: adapter, state: state} = conn, path, request_data, opts \\ []) do
    merged_opts = SimpleGrpc.Client.Headers.merge_opts(conn, opts)
    adapter.send_stream(state, path, request_data, merged_opts)
  end

  # ---------------------------------------------------------------------------
  # Internal Call Resolvers & Dispatch
  # ---------------------------------------------------------------------------

  defp resolve_call_schema(%Conn{schema: schema}, _rpc_or_path, opts) when not is_nil(schema) do
    {:ok, Keyword.get(opts, :schema, schema)}
  end

  defp resolve_call_schema(%Conn{} = conn, rpc_or_path, opts) do
    case Keyword.get(opts, :schema) do
      %SimpleGrpc.Schema{} = schema ->
        {:ok, schema}

      nil ->
        service_symbol = extract_service_symbol(rpc_or_path)

        refl_result =
          if service_symbol do
            SimpleGrpc.Client.Reflection.fetch_schema(conn, service_symbol, opts)
          else
            SimpleGrpc.Client.Reflection.fetch_schema(conn, opts)
          end

        case refl_result do
          {:ok, schema} when map_size(schema.services) > 0 or map_size(schema.messages) > 0 ->
            {:ok, schema}

          {:ok, _empty} ->
            {:error, :no_schema,
             "No schema attached to connection and reflection returned empty schema"}

          {:error, reason} ->
            {:error, :no_schema,
             "No schema attached to connection and reflection discovery failed: #{inspect(reason)}"}
        end
    end
  end

  defp resolve_call_rpc(%SimpleGrpc.Schema{} = schema, path) when is_binary(path) do
    normalized_path = if String.starts_with?(path, "/"), do: path, else: "/#{path}"

    case SimpleGrpc.Schema.find_rpc(schema, normalized_path) do
      {_svc, rpc} ->
        {:ok, rpc}

      nil ->
        parts = String.split(String.trim_leading(path, "/"), "/")

        if length(parts) == 2 do
          [svc_name, method_name] = parts
          service = SimpleGrpc.Schema.service(schema, svc_name)

          if service do
            find_rpc_in_service(service, method_name)
          else
            {:error, :service_not_found, "Service #{inspect(svc_name)} not found in schema"}
          end
        else
          {:error, :rpc_not_found, "RPC #{inspect(path)} not found in schema"}
        end
    end
  end

  defp resolve_call_rpc(%SimpleGrpc.Schema{} = schema, {svc_name, method_name}) do
    service = SimpleGrpc.Schema.service(schema, svc_name)

    if service do
      find_rpc_in_service(service, to_string(method_name))
    else
      {:error, :service_not_found, "Service #{inspect(svc_name)} not found in schema"}
    end
  end

  defp find_rpc_in_service(%SimpleGrpc.Schema.Service{rpcs: rpcs, name: svc_name}, method_name) do
    method_str = to_string(method_name)
    snake_str = Macro.underscore(method_str)

    found_rpc =
      Map.get(rpcs, method_str) ||
        Map.get(rpcs, snake_str) ||
        Enum.find_value(rpcs, fn {_k, r} ->
          if r.name == method_str or Macro.underscore(r.name) == snake_str, do: r
        end)

    if found_rpc do
      {:ok, found_rpc}
    else
      {:error, :method_not_found,
       "Method #{inspect(method_name)} not found in service #{inspect(svc_name)}"}
    end
  end

  defp extract_service_symbol(path) when is_binary(path) do
    parts = String.split(String.trim_leading(path, "/"), "/")
    if length(parts) >= 2, do: List.first(parts), else: nil
  end

  defp extract_service_symbol({svc, _method}), do: to_string(svc)
  defp extract_service_symbol(_), do: nil

  defp invoke_dynamic_rpc(conn, %SimpleGrpc.Schema.RPC{} = rpc, payload, schema, opts) do
    merged_opts = SimpleGrpc.Client.Headers.merge_opts(conn, opts)
    headers = Keyword.get(merged_opts, :headers, [])
    service_full_name = extract_service_prefix(rpc.path)
    service_name = extract_service_name(service_full_name)

    conn_interceptors = (conn && conn.opts && Keyword.get(conn.opts, :interceptors)) || []
    call_interceptors = Keyword.get(opts, :interceptors, [])
    all_interceptors = List.wrap(conn_interceptors) ++ List.wrap(call_interceptors)

    ctx = %SimpleGrpc.Context{
      role: :client,
      service: service_name,
      service_name: service_full_name,
      rpc: rpc.name,
      rpc_name: rpc.name,
      path: rpc.path,
      request_stream: rpc.request_stream,
      response_stream: rpc.response_stream,
      headers: headers,
      metadata: Keyword.get(opts, :metadata, %{}),
      private: %{},
      opts: merged_opts,
      conn: conn,
      schema: schema
    }

    SimpleGrpc.Pipeline.execute(all_interceptors, payload, ctx, fn req, current_ctx ->
      call_opts = Keyword.put(current_ctx.opts, :headers, current_ctx.headers)

      meta = %{
        service: current_ctx.service,
        service_full_name: current_ctx.service_name,
        rpc: current_ctx.rpc_name,
        path: current_ctx.path,
        headers: current_ctx.headers,
        request: req,
        adapter: conn.adapter,
        conn: conn
      }

      SimpleGrpc.Client.Telemetry.span(meta, fn ->
        if rpc.request_stream or rpc.response_stream do
          encoded_data =
            if rpc.request_stream do
              cond do
                is_struct(req, Stream) or is_function(req, 2) or is_list(req) ->
                  Stream.map(req, fn item ->
                    SimpleGrpc.Protobuf.encode!(rpc.input, item, schema)
                  end)

                true ->
                  [SimpleGrpc.Protobuf.encode!(rpc.input, req, schema)]
              end
            else
              SimpleGrpc.Protobuf.encode!(rpc.input, req, schema)
            end

          case send_stream(conn, rpc.path, encoded_data, call_opts) do
            {:ok, resp_stream, _headers, _trailers} ->
              if rpc.response_stream do
                decoded_stream =
                  Stream.map(resp_stream, fn chunk ->
                    SimpleGrpc.Protobuf.decode!(rpc.output, chunk, schema)
                  end)

                {:ok, decoded_stream}
              else
                chunk =
                  cond do
                    is_binary(resp_stream) ->
                      resp_stream

                    is_struct(resp_stream, Stream) or is_function(resp_stream, 2) or
                        is_list(resp_stream) ->
                      Enum.at(resp_stream, 0) || <<>>

                    true ->
                      <<>>
                  end

                case SimpleGrpc.Protobuf.decode(rpc.output, chunk, schema) do
                  {:ok, decoded_map} ->
                    {:ok, decoded_map}

                  {:error, _code, reason} ->
                    {:error, 2, "Failed to decode response: #{reason}"}
                end
              end

            {:error, status_code, status_msg} ->
              {:error, status_code, status_msg}

            {:error, reason} ->
              {:error, 2, inspect(reason)}
          end
        else
          req_bytes = SimpleGrpc.Protobuf.encode!(rpc.input, req, schema)

          case send_unary(conn, rpc.path, req_bytes, call_opts) do
            {:ok, resp_bytes, _headers, _trailers} ->
              case SimpleGrpc.Protobuf.decode(rpc.output, resp_bytes, schema) do
                {:ok, decoded_map} ->
                  {:ok, decoded_map}

                {:error, _code, reason} ->
                  {:error, 2, "Failed to decode response: #{reason}"}
              end

            {:error, status_code, status_msg} ->
              {:error, status_code, status_msg}

            {:error, reason} ->
              {:error, 2, inspect(reason)}
          end
        end
      end)
    end)
  end

  defp extract_service_prefix(path) do
    trimmed = String.trim_leading(path, "/")

    case String.split(trimmed, "/") do
      [prefix, _rpc] -> prefix
      _ -> trimmed
    end
  end

  defp extract_service_name(service_prefix) do
    service_prefix
    |> String.split(".")
    |> List.last()
  end

  defp resolve_adapter(target, opts) do
    case Keyword.get(opts, :adapter) do
      :mint ->
        SimpleGrpc.Client.Adapters.Mint

      :plug ->
        SimpleGrpc.Client.Adapters.Plug

      mod when is_atom(mod) and not is_nil(mod) ->
        mod

      nil ->
        if (is_atom(target) and function_exported?(target, :init, 1)) or
             Keyword.has_key?(opts, :plug) do
          SimpleGrpc.Client.Adapters.Plug
        else
          SimpleGrpc.Client.Adapters.Mint
        end
    end
  end
end
