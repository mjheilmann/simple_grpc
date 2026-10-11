defmodule SimpleGrpc.Client.Adapters.Plug do
  @moduledoc """
  In-memory client transport adapter for fast integration testing against `Plug` / `SimpleGrpc.Plug`.
  Executes requests directly without opening a TCP port or network socket.
  """

  @behaviour SimpleGrpc.Client.Adapter

  alias SimpleGrpc.Client.Framing

  @impl true
  def connect(target, opts \\ []) do
    {plug_mod, plug_opts} =
      cond do
        Keyword.has_key?(opts, :plug_opts) ->
          plug_mod = if is_atom(target), do: target, else: SimpleGrpc.Plug
          {plug_mod, Keyword.get(opts, :plug_opts)}

        is_atom(target) ->
          {target, target.init(opts)}

        is_tuple(target) and tuple_size(target) == 2 ->
          {elem(target, 0), elem(target, 0).init(elem(target, 1))}

        true ->
          {SimpleGrpc.Plug, SimpleGrpc.Plug.init(opts)}
      end

    {:ok, %{plug: plug_mod, plug_opts: plug_opts, opts: opts}}
  end

  @impl true
  def send_unary(%{plug: plug_mod, plug_opts: plug_opts}, path, payload, opts) do
    custom_headers = Keyword.get(opts, :headers, [])
    req_ct = Enum.find_value(custom_headers, "application/grpc", fn {k, v} ->
      if String.downcase(to_string(k)) == "content-type", do: to_string(v)
    end)

    framed_body = Framing.frame(payload, opts)
    final_body =
      if String.contains?(req_ct, "grpc-web-text") do
        Base.encode64(framed_body)
      else
        framed_body
      end

    conn =
      Plug.Test.conn(:post, path, final_body)
      |> Plug.Conn.put_req_header("content-type", req_ct)
      |> Plug.Conn.put_req_header("te", "trailers")
      |> apply_headers(custom_headers)
      |> plug_mod.call(plug_opts)

    headers = conn.resp_headers
    resp_ct = Enum.find_value(headers, "", fn {k, v} ->
      if String.downcase(to_string(k)) == "content-type", do: to_string(v)
    end)

    raw_body =
      if String.contains?(resp_ct, "grpc-web-text") do
        case Base.decode64(conn.resp_body, ignore: :whitespace) do
          {:ok, dec} -> dec
          _ -> conn.resp_body
        end
      else
        conn.resp_body
      end

    {status_code, status_msg, rich_status} = Framing.extract_grpc_status_details(headers)
    resp_encoding = extract_encoding(headers)

    case Framing.unframe(raw_body, encoding: resp_encoding) do
      {:ok, response_payload, rest} ->
        # Check if trailers were packed in body frame
        {final_code, final_msg, final_rich} =
          case Framing.unframe(rest) do
            {:trailers, trailers, _} ->
              {t_code, t_msg, t_rich} = Framing.extract_grpc_status_details(trailers)
              {t_code, t_msg, t_rich || rich_status}

            _ ->
              {status_code, status_msg, rich_status}
          end

        if final_code == 0 do
          {:ok, response_payload, headers, []}
        else
          if final_rich do
            {:error, final_code, final_msg, final_rich}
          else
            {:error, final_code, final_msg}
          end
        end

      {:trailers, trailers, _rest} ->
        {t_code, t_msg, t_rich} = Framing.extract_grpc_status_details(trailers)
        if t_rich do
          {:error, t_code, t_msg, t_rich}
        else
          {:error, t_code, t_msg}
        end

      {:incomplete, <<>>} ->
        if status_code == 0 do
          {:ok, <<>>, headers, []}
        else
          if rich_status, do: {:error, status_code, status_msg, rich_status}, else: {:error, status_code, status_msg}
        end

      {:incomplete, _} ->
        if status_code == 0 do
          {:ok, raw_body, headers, []}
        else
          if rich_status, do: {:error, status_code, status_msg, rich_status}, else: {:error, status_code, status_msg}
        end

      {:error, reason} ->
        if status_code != 0 do
          if rich_status, do: {:error, status_code, status_msg, rich_status}, else: {:error, status_code, status_msg}
        else
          {:error, 2, reason}
        end
    end
  end

  @impl true
  def send_stream(%{plug: plug_mod, plug_opts: plug_opts}, path, request_data, opts) do
    custom_headers = Keyword.get(opts, :headers, [])

    framed_body =
      cond do
        is_binary(request_data) ->
          Framing.frame(request_data, opts)

        is_struct(request_data, Stream) or is_list(request_data) or is_function(request_data, 2) ->
          request_data
          |> Enum.map(&Framing.frame(&1, opts))
          |> IO.iodata_to_binary()

        true ->
          <<>>
      end

    conn =
      Plug.Test.conn(:post, path, framed_body)
      |> Plug.Conn.put_req_header("content-type", "application/grpc")
      |> Plug.Conn.put_req_header("te", "trailers")
      |> apply_headers(custom_headers)
      |> plug_mod.call(plug_opts)

    headers = conn.resp_headers
    {status_code, status_msg, rich_status} = Framing.extract_grpc_status_details(headers)
    resp_encoding = extract_encoding(headers)

    if status_code == 0 do
      stream =
        Stream.unfold(conn.resp_body, fn
          <<>> ->
            nil

          binary when is_binary(binary) ->
            case Framing.unframe(binary, encoding: resp_encoding) do
              {:ok, payload, rest} -> {payload, rest}
              _ -> nil
            end
        end)

      {:ok, stream, headers, []}
    else
      if rich_status do
        {:error, status_code, status_msg, rich_status}
      else
        {:error, status_code, status_msg}
      end
    end
  end

  defp extract_encoding(headers) do
    Enum.find_value(headers, "gzip", fn {k, v} ->
      if String.downcase(to_string(k)) == "grpc-encoding", do: to_string(v)
    end)
  end

  @impl true
  def close(_conn), do: :ok

  @impl true
  def ping(_conn, _opts \\ []), do: {:ok, 0}

  defp apply_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {k, v}, acc ->
      Plug.Conn.put_req_header(acc, to_string(k), to_string(v))
    end)
  end
end
