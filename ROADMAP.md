# SimpleGrpc Production Roadmap

This roadmap outlines key architectural enhancements and features planned for `SimpleGrpc` to ensure high performance, enterprise reliability, and full specification compliance in production environments.

---

## 📌 Phase 1: High-Performance Networking & Reliability

- [x] **1. Client Connection Pooling (`SimpleGrpc.Client.Pool`)**
  - [x] Implement a partitioned connection pool (using `NimblePool`) over `Mint.HTTP2`.
  - [x] Distribute concurrent client calls across multiple multiplexed HTTP/2 connections to prevent single-GenServer mailbox bottlenecks.
  - [x] Support pool sizing and checkout timeout configuration in `SimpleGrpc.Client.connect/2`.
  - [x] Provide connection recycling and health monitoring for disconnected or failed socket processes.

- [ ] **2. HTTP/2 Keepalive & Ping Probes**
  - [ ] Client keepalive: periodically send HTTP/2 `PING` frames on idle connections to prevent NAT/ALB/NLB connection dropouts.
  - [ ] Server keepalive: respond to PINGs and optionally enforce minimum ping intervals (`permit_without_calls`, `min_ping_interval`).

- [ ] **3. Deadline & Timeout Propagation**
  - [ ] Server: Parse `grpc-timeout` header (e.g., `1000m`, `10S`) and bound handler execution to the deadline.
  - [ ] Server: Automatically cancel the underlying handler `Task` or abort stream consumption if the client disconnects or deadline expires.
  - [ ] Client: Calculate remaining deadline and forward `grpc-timeout` headers on downstream client calls.

- [ ] **4. Server Graceful Shutdown & Drain**
  - [ ] Trap exit / handle SIGTERM in the server supervision tree.
  - [ ] Send HTTP/2 `GOAWAY` frames and reject new RPCs with `UNAVAILABLE`.
  - [ ] Allow in-flight unary requests and active streams a configurable grace period (e.g., 30s) to complete before terminating.

---

## 📌 Phase 2: Protobuf Ecosystem & Specs

- [ ] **5. `proto2` Syntax Support**
  - [ ] Extend `SimpleGrpc.Parser` to support `syntax = "proto2"`.
  - [ ] Parse `required`, `optional` with explicit `default = ...`, `extensions`, and `extend` blocks.
  - [ ] Handle proto2 field presence semantics (tracking explicitly set fields vs. defaults).

- [ ] **6. Protobuf Editions (Editions 2023+)**
  - [ ] Support `edition = "2023";` syntax in parser and compiler.
  - [ ] Implement edition feature flags (`features.field_presence`, `features.enum_type`, etc.).

- [ ] **7. Well-Known Types (WKT) & Unknown Fields**
  - [ ] Native helpers/coercers for `google.protobuf.Timestamp`, `Duration`, `Struct`, `Any`, `Empty`, and `FieldMask`.
  - [ ] Unknown field preservation: retain unparsed wire tags during decode and re-emit them on encode (essential for proxies and backward compatibility).

---

## 📌 Phase 3: Middleware, Protocols & Error Handling

- [ ] **8. Client & Server Interceptor Pipeline**
  - [ ] **Client Interceptors**: Middleware stack for outgoing RPCs (`fn req, ctx, next -> next.(req, ctx)`).
  - [ ] **Server RPC Middleware**: Message-level hooks executed after Protobuf decoding and before handler dispatch.
  - [ ] **Built-in OpenTelemetry Support**: Automatic trace propagation (W3C `traceparent`) across client and server.

- [ ] **9. gRPC Payload Compression**
  - [ ] Support `grpc-encoding` and `grpc-accept-encoding` headers (`gzip`, `deflate`, `zstd`).
  - [ ] Encode/decode the 1-byte compression flag in the 5-byte gRPC framing header.

- [ ] **10. Rich Error Details (`google.rpc.Status`)**
  - [ ] Support `grpc-status-details-bin` trailing metadata.
  - [ ] Provide builders for standard error details (`ErrorInfo`, `BadRequest`, `RetryInfo`, `LocalizedMessage`).

---

## 📌 Phase 4: Developer Tooling & Web Interoperability

- [ ] **11. gRPC-Web Support**
  - [ ] Add `SimpleGrpc.Plug.GrpcWeb` supporting `application/grpc-web` and `application/grpc-web-text` (base64) over HTTP/1.1 and HTTP/2 for direct browser access.

- [ ] **12. Static Code Generation Mix Task**
  - [ ] Add `mix simple_grpc.gen` to generate static `.ex` files from `.proto` files into `lib/` for projects preferring offline compilation, Credo/Dialyzer checks, and explicit code generation.
