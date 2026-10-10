# SimpleGrpc Production Roadmap

This roadmap outlines key architectural enhancements and features planned for `SimpleGrpc` to ensure high performance, enterprise reliability, and full specification compliance in production environments.

---

## 📌 Phase 1: High-Performance Networking & Reliability

- [x] **1. Client Connection Pooling (`SimpleGrpc.Client.Pool`)**
  - [x] Implement a partitioned connection pool (using `NimblePool`) over `Mint.HTTP2`.
  - [x] Distribute concurrent client calls across multiple multiplexed HTTP/2 connections to prevent single-GenServer mailbox bottlenecks.
  - [x] Support pool sizing and checkout timeout configuration in `SimpleGrpc.Client.connect/2`.
  - [x] Provide connection recycling and health monitoring for disconnected or failed socket processes.

- [x] **2. HTTP/2 Keepalive & Ping Probes**
  - [x] Client keepalive: periodically send HTTP/2 `PING` frames on idle connections to prevent NAT/ALB/NLB connection dropouts.
  - [x] Server keepalive: respond to PINGs via Bandit/HTTP2 and support round-trip latency measurements.

- [x] **3. Deadline & Timeout Propagation**
  - [x] Server: Parse `grpc-timeout` header (e.g., `1000m`, `10S`) and bound handler execution to the deadline.
  - [x] Server: Automatically cancel the underlying handler `Task` and enforce deadlines on streaming emissions.
  - [x] Client: Calculate remaining deadline and forward `grpc-timeout` headers via `:deadline` and `:timeout` options.
  - [x] Helpers: Provide `SimpleGrpc.deadline/1` and `SimpleGrpc.remaining_timeout/2` for server handler introspection.

---

## 📌 Phase 2: Protobuf Ecosystem & Specs

- [x] **4. `proto2` Syntax Support**
  - [x] Extend `SimpleGrpc.Parser` to support `syntax = "proto2"`.
  - [x] Parse `required`, `optional` with explicit `default = ...`, `extensions`, and `extend` blocks.
  - [x] Handle proto2 field presence semantics (tracking explicitly set fields vs. defaults).

- [x] **5. Protobuf Editions (Editions 2023+)**
  - [x] Support `edition = "2023";` syntax in parser and compiler.
  - [x] Implement edition feature flags (`features.field_presence`, `features.enum_type`, etc.).

- [x] **6. Well-Known Types (WKT) & Unknown Fields**
  - [x] Native helpers/coercers for `google.protobuf.Timestamp`, `Duration`, `Struct`, `Any`, `Empty`, and `FieldMask`.
  - [x] Unknown field preservation: retain unparsed wire tags during decode and re-emit them on encode (essential for proxies and backward compatibility).

---

## 📌 Phase 3: Middleware, Protocols & Error Handling

- [x] **7. Client & Server Interceptor Pipeline**
  - [x] **Client Interceptors**: Middleware stack for outgoing RPCs (`fn req, ctx, next -> next.(req, ctx)`).
  - [x] **Server RPC Middleware**: Message-level hooks executed after Protobuf decoding and before handler dispatch.
  - [x] **Built-in OpenTelemetry Support**: Automatic trace propagation (W3C `traceparent`) across client and server.

- [x] **8. gRPC Payload Compression**
  - [x] Support `grpc-encoding` and `grpc-accept-encoding` headers (`gzip`, `deflate`, `zstd`).
  - [x] Encode/decode the 1-byte compression flag in the 5-byte gRPC framing header.

- [x] **9. Rich Error Details (`google.rpc.Status`)**
  - [x] Support `grpc-status-details-bin` trailing metadata.
  - [x] Provide builders for standard error details (`ErrorInfo`, `BadRequest`, `RetryInfo`, `LocalizedMessage`, `DebugInfo`, `QuotaFailure`, `PreconditionFailure`, `ResourceInfo`, `Help`, `RequestInfo`).

---

## 📌 Phase 4: Developer Tooling & Web Interoperability

- [ ] **10. gRPC-Web Support**
  - [ ] Add `SimpleGrpc.Plug.GrpcWeb` supporting `application/grpc-web` and `application/grpc-web-text` (base64) over HTTP/1.1 and HTTP/2 for direct browser access.

- [ ] **11. Static Code Generation Mix Task**
  - [ ] Add `mix simple_grpc.gen` to generate static `.ex` files from `.proto` files into `lib/` for projects preferring offline compilation, Credo/Dialyzer checks, and explicit code generation.
