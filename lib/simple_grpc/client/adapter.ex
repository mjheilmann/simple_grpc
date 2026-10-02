defmodule SimpleGrpc.Client.Adapter do
  @moduledoc """
  Behaviour for gRPC client transport adapters (e.g. Mint HTTP/2 network transport, in-memory Plug adapter).
  """

  @type target :: String.t() | {String.t(), :inet.port_number()} | module() | term()
  @type conn_state :: term()
  @type headers :: [{String.t(), String.t()}]

  @doc """
  Initializes or establishes a connection to the target server or endpoint.
  """
  @callback connect(target :: target(), opts :: keyword()) ::
              {:ok, conn_state()} | {:error, term()}

  @doc """
  Sends a unary RPC request.
  Expects the binary payload already protobuf-encoded.
  Returns `{:ok, response_bytes, headers, trailers}` or `{:error, grpc_status, grpc_message}` or `{:error, term()}`.
  """
  @callback send_unary(
              conn :: conn_state(),
              path :: String.t(),
              payload :: binary(),
              opts :: keyword()
            ) ::
              {:ok, binary(), headers(), headers()}
              | {:error, integer(), String.t()}
              | {:error, term()}

  @doc """
  Sends a streaming RPC request (Server Streaming, Client Streaming, or Bidirectional Streaming).
  `request_data` may be a single binary payload or an `Enumerable.t()` of binary payloads.
  Returns `{:ok, Enumerable.t() | binary(), headers(), headers()}` or error tuple.
  """
  @callback send_stream(
              conn :: conn_state(),
              path :: String.t(),
              request_data :: binary() | Enumerable.t(),
              opts :: keyword()
            ) ::
              {:ok, Enumerable.t() | binary(), headers(), headers()}
              | {:error, integer(), String.t()}
              | {:error, term()}

  @doc """
  Closes the connection and cleans up any allocated resources.
  """
  @callback close(conn :: conn_state()) :: :ok | {:error, term()}
end
