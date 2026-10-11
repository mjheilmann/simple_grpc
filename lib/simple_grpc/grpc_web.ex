defmodule SimpleGrpc.GrpcWeb do
  @moduledoc """
  Core support for the gRPC-Web specification.

  Handles content-type detection, Base64 text encoding/decoding,
  trailers-in-body framing (`0x80` flag), and CORS preflight / response headers
  for browser clients (e.g., `@connectrpc/connect-web`, `grpc-web`).
  """

  import Plug.Conn, only: [get_req_header: 2, put_resp_header: 3, send_resp: 3]
  alias SimpleGrpc.{Compression, Status}

  @trailer_flag 0x80

  @grpc_web_content_types [
    "application/grpc-web",
    "application/grpc-web+proto",
    "application/grpc-web+json",
    "application/grpc-web-text",
    "application/grpc-web-text+proto",
    "application/grpc-web-text+json"
  ]

  @default_exposed_headers [
    "grpc-status",
    "grpc-message",
    "grpc-status-details-bin",
    "grpc-encoding",
    "grpc-accept-encoding",
    "x-grpc-web",
    "content-type"
  ]

  @default_allowed_headers [
    "content-type",
    "x-grpc-web",
    "x-user-agent",
    "grpc-timeout",
    "authorization",
    "x-requested-with",
    "keep-alive"
  ]

  @doc """
  Returns `true` if the given `Plug.Conn` or content-type string represents a gRPC-Web request.
  """
  def grpc_web_request?(%Plug.Conn{} = conn) do
    content_type = get_req_header(conn, "content-type") |> List.first()
    has_x_grpc_web = get_req_header(conn, "x-grpc-web") != []

    grpc_web_request?(content_type) or (has_x_grpc_web and is_binary(content_type) and String.starts_with?(content_type, "application/grpc"))
  end

  def grpc_web_request?(content_type) when is_binary(content_type) do
    normalized = content_type |> String.downcase() |> String.trim()

    Enum.any?(@grpc_web_content_types, fn ct ->
      String.starts_with?(normalized, ct)
    end)
  end

  def grpc_web_request?(_), do: false

  @doc """
  Determines the gRPC-Web format (`:binary`, `:text`, or `nil` if not gRPC-Web).
  """
  def format(%Plug.Conn{} = conn) do
    content_type = get_req_header(conn, "content-type") |> List.first()
    format(content_type)
  end

  def format(content_type) when is_binary(content_type) do
    normalized = content_type |> String.downcase() |> String.trim()

    cond do
      String.contains?(normalized, "grpc-web-text") ->
        :text

      String.contains?(normalized, "grpc-web") ->
        :binary

      true ->
        nil
    end
  end

  def format(_), do: nil

  @doc """
  Returns `true` if the content type or Conn uses Base64 text encoding (`application/grpc-web-text*`).
  """
  def text_format?(conn_or_content_type) do
    format(conn_or_content_type) == :text
  end

  @doc """
  Frames trailers into a standard gRPC-Web trailer frame (`0x80` flag prefix).
  """
  def frame_trailers(trailers) when is_list(trailers) do
    lines =
      trailers
      |> Enum.map(fn {k, v} -> "#{to_string(k)}: #{to_string(v)}\r\n" end)
      |> IO.iodata_to_binary()

    <<@trailer_flag::8, byte_size(lines)::32-big, lines::binary>>
  end

  def frame_trailers(status_code, status_message, details \\ nil) do
    code_int = Status.code(status_code)
    msg_str = to_string(status_message)

    details_bin =
      cond do
        is_list(details) and details != [] ->
          Status.to_status_details_bin({code_int, msg_str, details})

        is_binary(details) and details != "" ->
          details

        true ->
          nil
      end

    base_trailers = [
      {"grpc-status", to_string(code_int)},
      {"grpc-message", URI.encode(msg_str)}
    ]

    trailers =
      if details_bin do
        base_trailers ++ [{"grpc-status-details-bin", details_bin}]
      else
        base_trailers
      end

    frame_trailers(trailers)
  end

  @doc """
  Parses a raw trailer frame payload string (HTTP/1-style header lines) into a list of `{name, value}` tuples.
  """
  def parse_trailers_block(binary) when is_binary(binary) do
    binary
    |> String.split(["\r\n", "\n"], trim: true)
    |> Enum.flat_map(fn line ->
      case String.split(line, ":", parts: 2) do
        [k, v] -> [{String.trim(k), String.trim(v)}]
        _ -> []
      end
    end)
  end

  @doc """
  Unframes a gRPC-Web message or trailer frame from binary.
  Returns:
  - `{:data, payload, rest}` for normal payload frames
  - `{:trailers, trailers_list, rest}` for trailer frames (`0x80`)
  - `{:incomplete, binary}` if more data is needed
  - `{:eof, <<>>}` if binary is empty
  - `{:error, reason}` on invalid framing
  """
  def unframe(binary, opts \\ [])

  def unframe(<<>>, _opts), do: {:eof, <<>>}

  def unframe(
        <<flag::8, length::32-big, payload::binary-size(length), rest::binary>>,
        opts
      ) do
    case flag do
      0 ->
        {:data, payload, rest}

      1 ->
        encoding = Keyword.get(opts, :encoding, "gzip")

        case Compression.decompress(payload, encoding) do
          {:ok, decompressed} ->
            {:data, decompressed, rest}

          {:error, reason} ->
            {:error, "Failed to decompress gRPC-Web payload (#{inspect(encoding)}): #{reason}"}
        end

      @trailer_flag ->
        trailers = parse_trailers_block(payload)
        {:trailers, trailers, rest}

      other ->
        {:error, "Unsupported gRPC-Web frame flag: #{other}"}
    end
  end

  def unframe(binary, _opts) when is_binary(binary) do
    {:incomplete, binary}
  end

  @doc """
  Decodes a gRPC-Web request body.
  If the format is `:text`, decodes from Base64 (ignoring whitespace).
  Otherwise returns the binary as is.
  """
  def decode_request_body(body, format_or_content_type) do
    if format_or_content_type == :text or text_format?(format_or_content_type) do
      case Base.decode64(body, ignore: :whitespace) do
        {:ok, decoded} -> {:ok, decoded}
        :error -> {:error, "Invalid base64 encoding in gRPC-Web request body"}
      end
    else
      {:ok, body}
    end
  end

  @doc """
  Encodes a gRPC-Web response body.
  If the format is `:text`, encodes to Base64.
  Otherwise returns the binary as is.
  """
  def encode_response_body(body, format_or_content_type) do
    if format_or_content_type == :text or text_format?(format_or_content_type) do
      Base.encode64(body)
    else
      body
    end
  end

  @doc """
  Returns `true` if the request is an HTTP `OPTIONS` CORS preflight request.
  """
  def cors_preflight?(%Plug.Conn{method: "OPTIONS"} = conn) do
    get_req_header(conn, "access-control-request-method") != [] or
      get_req_header(conn, "origin") != []
  end

  def cors_preflight?(_), do: false

  @doc """
  Handles an HTTP `OPTIONS` CORS preflight request, setting the appropriate headers and returning 204 No Content.
  """
  def handle_cors_preflight(%Plug.Conn{} = conn, opts \\ []) do
    conn
    |> put_cors_headers(opts)
    |> send_resp(204, "")
  end

  @doc """
  Puts CORS headers on a `Plug.Conn` response.
  """
  def put_cors_headers(%Plug.Conn{} = conn, opts \\ []) do
    origin_header = get_req_header(conn, "origin") |> List.first()

    allow_origin =
      case Keyword.get(opts, :allowed_origins, "*") do
        "*" ->
          "*"

        :origin ->
          origin_header || "*"

        origins when is_list(origins) ->
          if origin_header in origins, do: origin_header, else: List.first(origins) || "*"

        origin when is_binary(origin) ->
          origin
      end

    req_headers = get_req_header(conn, "access-control-request-headers") |> List.first()

    allow_headers =
      case Keyword.get(opts, :allowed_headers, :any) do
        :any ->
          if req_headers && req_headers != "",
            do: req_headers,
            else: Enum.join(@default_allowed_headers, ", ")

        headers when is_list(headers) ->
          Enum.join(headers, ", ")

        headers when is_binary(headers) ->
          headers
      end

    expose_headers =
      case Keyword.get(opts, :exposed_headers, @default_exposed_headers) do
        headers when is_list(headers) -> Enum.join(headers, ", ")
        headers when is_binary(headers) -> headers
      end

    max_age = Keyword.get(opts, :max_age, 86400) |> to_string()

    conn
    |> put_resp_header("access-control-allow-origin", allow_origin)
    |> put_resp_header("access-control-allow-methods", "POST, OPTIONS")
    |> put_resp_header("access-control-allow-headers", allow_headers)
    |> put_resp_header("access-control-expose-headers", expose_headers)
    |> put_resp_header("access-control-max-age", max_age)
  end

  @doc """
  Resolves the appropriate response content-type for a gRPC-Web request.
  """
  def response_content_type(req_content_type) when is_binary(req_content_type) do
    normalized = String.downcase(req_content_type)

    cond do
      String.contains?(normalized, "grpc-web-text+proto") ->
        "application/grpc-web-text+proto"

      String.contains?(normalized, "grpc-web-text+json") ->
        "application/grpc-web-text+json"

      String.contains?(normalized, "grpc-web-text") ->
        "application/grpc-web-text"

      String.contains?(normalized, "grpc-web+proto") ->
        "application/grpc-web+proto"

      String.contains?(normalized, "grpc-web+json") ->
        "application/grpc-web+json"

      String.contains?(normalized, "grpc-web") ->
        "application/grpc-web"

      true ->
        "application/grpc-web+proto"
    end
  end

  def response_content_type(_), do: "application/grpc-web+proto"
end
