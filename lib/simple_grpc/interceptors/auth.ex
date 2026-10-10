defmodule SimpleGrpc.Interceptors.Auth do
  @moduledoc """
  Authentication interceptor supporting client token injection and server token validation.

  ## Client Options
  - `:bearer` - Bearer token string (injected as `authorization: Bearer <token>`)
  - `:token` - Raw token string (injected as `authorization: <token>`)
  - `:header` - Custom header name (default: `"authorization"`)

  ## Server Options
  - `:token` - Expected token string or list of valid token strings
  - `:bearer` - Expected bearer token string or list of valid bearer strings
  - `:header` - Custom header name to inspect (default: `"authorization"`)
  - `:validator` - Validator function `(token, ctx -> :ok | {:ok, user} | boolean() | {:error, reason})`
  - `:status_code` - Status code returned on authentication failure (default: `16` / `:unauthenticated`)
  """

  @behaviour SimpleGrpc.Interceptor

  alias SimpleGrpc.Context

  @impl true
  def intercept(req, ctx, next, opts \\ [])

  def intercept(req, %Context{role: :client} = ctx, next, opts) do
    header_name = Keyword.get(opts, :header, "authorization")

    ctx =
      cond do
        bearer = Keyword.get(opts, :bearer) ->
          Context.put_header(ctx, header_name, "Bearer #{bearer}")

        token = Keyword.get(opts, :token) ->
          Context.put_header(ctx, header_name, to_string(token))

        true ->
          ctx
      end

    next.(req, ctx)
  end

  def intercept(req, %Context{} = ctx, next, opts) do
    header_name = Keyword.get(opts, :header, "authorization")
    header_val = Context.get_header(ctx, header_name)
    status_code = Keyword.get(opts, :status_code, 16)

    case validate_server_auth(header_val, ctx, opts) do
      :ok ->
        next.(req, ctx)

      {:ok, user} ->
        ctx = Context.put_private(ctx, :auth_user, user)
        next.(req, ctx)

      {:error, reason} ->
        {:error, status_code, "Unauthenticated: #{reason}"}
    end
  end

  defp validate_server_auth(nil, _ctx, _opts) do
    {:error, "missing authorization header"}
  end

  defp validate_server_auth(header_val, ctx, opts) do
    cond do
      validator = Keyword.get(opts, :validator) ->
        case validator.(header_val, ctx) do
          true -> :ok
          false -> {:error, "invalid credentials"}
          :ok -> :ok
          {:ok, user} -> {:ok, user}
          {:error, msg} -> {:error, msg}
          _ -> {:error, "unauthorized"}
        end

      expected_bearer = Keyword.get(opts, :bearer) ->
        expected_list = List.wrap(expected_bearer) |> Enum.map(&"Bearer #{&1}")

        if header_val in expected_list do
          :ok
        else
          {:error, "invalid bearer token"}
        end

      expected_token = Keyword.get(opts, :token) ->
        expected_list = List.wrap(expected_token) |> Enum.map(&to_string/1)

        if header_val in expected_list do
          :ok
        else
          {:error, "invalid token"}
        end

      true ->
        :ok
    end
  end
end
