# Concepts

> **Status: design in progress.** This page describes the model `simple_grpc` is
> converging on. The [status table](#current-status) at the end lists what is
> implemented today; the `README` examples describe the current API.

`simple_grpc` aims to be small and explicit. A few ideas carry the whole library.

## 1. Proto files are an explicit list

You give the library every `.proto` file that defines your service, as a list.
There is no `protoc`, and there are no include paths or root folders to configure.

```elixir
use SimpleGrpc.Server,
  files: [
    "priv/protos/greeter/service.proto",
    "priv/protos/greeter/message.proto",
    "priv/protos/common/types.proto"
  ],
  service: "Greeter"
```

- Each `import "greeter/message.proto"` in a listed file must be satisfied by the list
  (the import string must match the end of exactly one listed path) or be a bundled
  well-known type such as `google/protobuf/timestamp.proto`.
- A missing import, an ambiguous match, or two files in the list defining the same
  fully-qualified name is a **compile error**. Nothing is silently ignored.
- Editing a listed `.proto` recompiles the module that lists it.
- Entries may be globs: `"priv/protos/greeter/*.proto"` stands for every `.proto` file it
  matches (sorted; a glob that matches nothing is a compile error). Adding or removing a
  matching file recompiles the module too.

## 2. Service and server

Two words, two meanings:

- **service** is the term from the proto definition (`service Greeter { ... }`).
- **server** is *your implementation module* of one service.

`service: "Greeter"` on `use` only selects which `service` block in the files this
module implements. It is required only when the files define more than one service,
and it is not an identifier used anywhere else. A server module implements exactly
one service.

```elixir
defmodule MyApp.GreeterServer do
  use SimpleGrpc.Server, files: [...], service: "Greeter"

  def say_hello(%MyApp.GreeterServer.Greeter.HelloRequest{} = req, _context) do
    {:ok, %{message: "Hello, " <> req.name}}
  end
end
```

The module owns its file set, generates its message structs, and holds the handlers.
It is not itself a plug. It exposes what plugs need through the `SimpleGrpc.Service`
behaviour (schema, service name and prefix, RPC list, interceptors, dispatch), so
the same module can be served by several plugs or ports and tested in memory.

## 3. Messages are structs

Each server module generates a struct for every message its service can reach (the RPC
input and output types and, transitively, every message type their fields mention),
namespaced under the module and qualified by proto package. Other messages in the listed
files, such as another service's, get no struct and cannot cause a name collision.
Handlers receive these structs.
Responses may be structs, maps, or keyword lists; the encoder checks them against the
schema.

Each server has **its own message identity**. Two servers built from overlapping files
get separate, equally valid struct types, even for the same proto message. This keeps
messages that look alike but are not alike from colliding. Helpers shared between
servers should return plain maps.

## 4. Plugs compose

There is no global registry. Each plug does one small job and you order them in an
ordinary `Plug.Builder` pipeline:

```elixir
defmodule MyApp.GrpcPipeline do
  use Plug.Builder

  plug SimpleGrpc.Plug.Service, server: MyApp.GreeterServer
  plug SimpleGrpc.Plug.Service, server: MyApp.AdminServer
  plug SimpleGrpc.Plug.Health
  plug SimpleGrpc.Plug.Reflection, servers: [MyApp.GreeterServer, MyApp.AdminServer]
  plug SimpleGrpc.Plug.NotFound
end

children = [{Bandit, plug: MyApp.GrpcPipeline, port: 50051}]
```

- **`Plug.Service`** serves exactly one service. It claims requests under
  `/<package>.<Service>/` (with the trailing slash), answers an unknown method under
  that prefix with `UNIMPLEMENTED`, and **passes everything else through**. A request
  it handles is halted.
- **`Plug.Health`** and **`Plug.Reflection`** are ordinary service plugs that ship with
  the library. Reflection is told which servers it may describe, so different plugs or
  ports can expose different sets.
- **`Plug.NotFound`** goes last. It answers an unknown service with `UNIMPLEMENTED`
  (gRPC clients) or HTTP 404/415 (non-gRPC clients). Without it, a request nothing
  claims gets no gRPC-shaped response.
- Order is the only routing configuration: put a plug earlier and it gets first claim.
- Cross-cutting concerns (CORS, gRPC-Web translation) are their own plugs placed in
  front of the service plugs.

## 5. Reflection works on descriptors

Reflection never touches your structs. It builds a descriptor pool from the listed
servers, keyed by the *logical* filename (the exact string used in `import`). Identical
files from different servers are deduplicated by name and content. The same filename
or symbol with **different** content fails at startup; put servers with divergent
versions behind separate reflection plugs.

## 6. Handler contract

A handler is named after the RPC in `snake_case` and takes `(request, context)`:

| Return value | Meaning |
| :--- | :--- |
| `{:ok, response}` or a bare struct/map | Success |
| `{:error, code, message}` / `{:error, code, message, details}` | gRPC error with a numeric status code |
| An `Enumerable` (server and bidirectional streaming) | A lazy stream of responses |

For client and bidirectional streaming, `request` is a lazy stream of decoded messages.

## Current status

| Concept | Today | Target |
| :--- | :--- | :--- |
| Explicit `files:` list, import validation | Implemented: `files:` list (any compile-time expression, glob entries supported), imports and duplicate names validated, `server_file:` kept as shorthand. `service:` is required when the files define several services. | Section 1 |
| Structs in handlers | Structs are generated, but the Plug path passes plain maps | Section 3 |
| Package-qualified, per-server structs | Implemented: every message the service reaches gets a struct under the server module, qualified by proto package (`MyApp.GreeterServer.Greeter.HelloRequest`); a name collision is a compile error. Well-known types stay plain maps | Section 3 |
| Plug split | One `SimpleGrpc.Plug` with `services:`, `unmatched:`, `reflection:` and `health:` options, plus a global `SimpleGrpc.Registry` | Section 4 |
| `SimpleGrpc.Service` behaviour | Informal `__schema__/0`, `__rpcs__/0`, `route_info/1` functions | Section 2 |
| Reflection by logical filename | Matches schemas by file basename and package heuristics | Section 5 |
