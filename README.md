# SimpleGrpc

[![CI](https://github.com/mjheilmann/simple_grpc/actions/workflows/ci.yml/badge.svg)](https://github.com/mjheilmann/simple_grpc/actions/workflows/ci.yml)

A lightweight, high-performance, and purely idiomatic gRPC framework for Elixir.

- **Zero External Compilers**: No `protoc` binary or C/NIF dependencies required. Parses `.proto` files directly in pure Elixir at compile-time or runtime.
- **Plug & Bandit Native**: Operates as a standard Elixir `Plug` over HTTP/2 with [Bandit](https://github.com/mtrudel/bandit).
- **Pure Elixir HTTP/2 Client**: Native HTTP/2 client transport powered by [Mint](https://github.com/elixir-mint/mint).
- **Lightning-Fast In-Memory Testing**: Includes `SimpleGrpc.Case` and an in-memory Plug adapter to test services with zero TCP overhead.
- **Built-in Reflection**: First-class support for `grpc.reflection.v1` and `grpc.reflection.v1alpha` out of the box.
- **Full Streaming Support**: Unary, Server Streaming, Client Streaming, and Bidirectional Streaming.
- **Telemetry Ready**: Native `:telemetry` instrumentation on both client and server.

---

## Installation

Add `simple_grpc` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:simple_grpc, "~> 0.1.0"},
    {:bandit, "~> 1.0"}
  ]
end
```

---

## Concepts

How `simple_grpc` thinks about proto files, services versus servers, messages and plugs
is described in [docs/concepts.md](docs/concepts.md). It also lists which parts of that
model are implemented today; the examples below show the current API.

---

## Defining a Server

Define a gRPC server module pointing to your `.proto` file. `server_file:` is the shorthand
for a single self-contained file. A service that imports other files lists all of them with
`files: [...]` instead; entries may be globs like `"priv/protos/greeter/*.proto"` (see
[docs/concepts.md](docs/concepts.md)):

```elixir
defmodule MyEchoServer do
  use SimpleGrpc.Server,
    server_file: "priv/protos/echo_service.proto"

  # In-module handler for Echo RPC
  def echo(%{message: msg}, _stream) do
    {:ok, %{reply: "Echo: " <> msg}}
  end
end
```

### Running with Bandit / Plug

Add `SimpleGrpc.Plug` to your application supervision tree:

```elixir
# In application.ex or endpoint
children = [
  {Bandit, plug: SimpleGrpc.Plug, port: 50051}
]

# Register services with SimpleGrpc
SimpleGrpc.register_service(MyEchoServer)
```

You can also isolate services to specific Plugs or enable gRPC Server Reflection:

```elixir
# Plug with isolated services and scoped reflection
plug SimpleGrpc.Plug,
  services: [MyEchoServer],
  reflection: [only: [MyEchoServer]]
```

---

## Streaming (Lazy, Memory-Efficient & Concurrent)

`SimpleGrpc` handles all 4 gRPC streaming patterns lazily over HTTP/2 without buffering entire streams into memory:

| Pattern | Handler Signature | Data Flow & Memory |
| :--- | :--- | :--- |
| **Unary** | `def call(req, _conn) -> {:ok, %{...}}` | Single bounded request/response. |
| **Server Streaming** | `def call(req, _conn) -> Enumerable.t()` | Emits a lazy `Stream`. Each item is immediately encoded and flushed via HTTP/2 chunking ($O(1)$ memory). |
| **Client Streaming** | `def call(req_stream, _conn) -> {:ok, %{...}}` | Receives a lazy `Stream`. Frames are read and decoded on demand as the handler iterates (e.g. `Enum.reduce/3`). |
| **Bidirectional Streaming** | `def call(req_stream, _conn) -> Enumerable.t()` | Consumes request frames and emits response chunks concurrently over the active HTTP/2 stream. |

### Streaming Server Example

```elixir
defmodule MyStreamingServer do
  use SimpleGrpc.Server,
    server_file: "priv/protos/streaming_service.proto"

  # 1. Server Streaming: Single request -> lazy Stream of responses
  def server_streaming_call(req, _conn) do
    1..10_000
    |> Stream.map(fn seq ->
      %{result: "Data chunk #{seq} for #{req.message}", sequence: seq, is_final: seq == 10_000}
    end)
  end

  # 2. Client Streaming: Lazy request Stream -> single response
  def client_streaming_call(req_stream, _conn) do
    {count, total_seq} =
      Enum.reduce(req_stream, {0, 0}, fn req, {c, s} ->
        {c + 1, s + req.sequence}
      end)

    {:ok, %{result: "Processed #{count} items", sequence: total_seq, is_final: true}}
  end

  # 3. Bidirectional Streaming: Lazy request Stream -> lazy response Stream
  def bidirectional_streaming_call(req_stream, _conn) do
    Stream.map(req_stream, fn req ->
      %{result: "Echo: " <> req.message, sequence: req.sequence * 2, is_final: false}
    end)
  end
end
```

### Why Lazy Streaming?

1. **$O(1)$ Memory Footprint**: Messages are read and written frame-by-frame. You can stream gigabytes of telemetry or millions of rows with minimal RAM usage.
2. **Immediate Time-to-First-Byte (TTFB)**: Response chunks begin arriving at the client within milliseconds of being generated.
3. **Natural Backpressure**: Because request reading is demand-driven via `Stream.resource`, slow consumer processing pauses socket consumption, automatically throttling the producer via standard HTTP/2 flow control windows.

---

## Testing with In-Memory Plug (`SimpleGrpc.Case`)

`SimpleGrpc` provides `SimpleGrpc.Case` (an `ExUnit.CaseTemplate`) for testing your gRPC services and plugs directly in-memory without starting Bandit or binding a TCP port.

### Basic Integration Test

```elixir
defmodule MyEchoServerTest do
  use SimpleGrpc.Case, plug: SimpleGrpc.Plug

  setup do
    SimpleGrpc.Registry.clear()
    SimpleGrpc.register_service(MyEchoServer)
    :ok
  end

  test "invokes Echo RPC in-memory", %{conn: conn} do
    # 1. Encode protobuf request payload
    schema = MyEchoServer.__schema__()
    req_bytes = SimpleGrpc.Protobuf.encode!("EchoRequest", %{message: "Ping"}, schema)

    # 2. Call the service in-memory via grpc_call helper
    {:ok, resp_bytes, headers, _trailers} =
      grpc_call(conn, "/echo.EchoService/Echo", req_bytes)

    # 3. Decode and assert response
    {:ok, resp} = SimpleGrpc.Protobuf.decode("EchoResponse", resp_bytes, schema)
    assert resp.reply == "Echo: Ping"
  end
end
```

### Testing Streaming Calls In-Memory

Use `grpc_stream/4` to test server, client, or bidirectional streaming:

```elixir
test "tests server streaming in-memory", %{conn: conn} do
  schema = MyStreamingServer.__schema__()
  req_bytes = SimpleGrpc.Protobuf.encode!("StreamRequest", %{message: "Start"}, schema)

  {:ok, resp_stream, _headers, _trailers} =
    grpc_stream(conn, "/streaming.StreamingService/ServerStreamingCall", req_bytes)

  items =
    resp_stream
    |> Enum.map(fn chunk ->
      {:ok, decoded} = SimpleGrpc.Protobuf.decode("StreamResponse", chunk, schema)
      decoded
    end)

  assert length(items) == 3
end
```

### Custom Plug Options & Per-Test Overrides

You can configure options globally in `use SimpleGrpc.Case` or override them per-test using ExUnit tags:

```elixir
defmodule CustomPlugTest do
  use SimpleGrpc.Case, plug: CustomGrpcPlug

  # Overrides the plug target specifically for this test case
  @tag plug: IsolatedServicePlug
  test "calls isolated plug", %{conn: conn} do
    # conn is connected directly to IsolatedServicePlug
    {:ok, resp_bytes, _h, _t} = grpc_call(conn, "/custom.Service/Action", req_bytes)
    assert resp_bytes != nil
  end
end
```

---

## Client Usage

`SimpleGrpc.Client` connects over network HTTP/2 (via Mint) or directly in-memory (via Plug):

```elixir
# 1. Connect over HTTP/2 TCP
{:ok, conn} = SimpleGrpc.Client.connect("127.0.0.1:50051", adapter: :mint)

# 2. Send Unary RPC
{:ok, resp_bytes, headers, trailers} =
  SimpleGrpc.Client.send_unary(conn, "/echo.EchoService/Echo", req_bytes)

# 3. Send Streaming RPC
{:ok, resp_stream, headers, trailers} =
  SimpleGrpc.Client.send_stream(conn, "/streaming.StreamingService/ServerStreamingCall", req_bytes)

# 4. Close connection
:ok = SimpleGrpc.Client.close(conn)
```

---

## gRPC Reflection

`SimpleGrpc` includes built-in reflection compatible with `grpcurl`, Postman, and BloomRPC:

```elixir
# Enable reflection on all registered services
SimpleGrpc.enable_reflection()
```

Inspect services and invoke methods without providing `.proto` files:

```bash
# Discover services
grpcurl -plaintext localhost:50051 list

# Describe a service
grpcurl -plaintext localhost:50051 describe streaming.StreamingService

# Invoke an RPC directly via reflection
grpcurl -plaintext -d '{"message": "Hello!"}' localhost:50051 streaming.StreamingService/UnaryCall
```

---

## Telemetry Events

`SimpleGrpc` emits `:telemetry` events on both the server and client:

- Server Lifecycle:
  - `[:simple_grpc, :call, :start]` — `%{path: path, headers: req_headers}`
  - `[:simple_grpc, :call, :stop]` — measurements: `%{duration: duration}`, metadata: `%{path: path, request: req, response: resp, grpc_status: status}`
  - `[:simple_grpc, :call, :exception]` — `%{path: path, kind: kind, reason: reason, stacktrace: stacktrace}`

