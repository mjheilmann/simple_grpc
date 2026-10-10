defmodule SimpleGrpc.Context do
  @moduledoc """
  Unified execution context passed across client and server interceptor pipelines and handler functions.

  ## Fields
  - `:role` - `:client` or `:server`
  - `:service` - Unqualified service name (e.g. `"Greeter"`)
  - `:service_name` - Full service package/name (e.g. `"helloworld.Greeter"`)
  - `:rpc` - RPC name as atom or string (e.g. `:say_hello` or `"SayHello"`)
  - `:rpc_name` - Canonical RPC method name (e.g. `"SayHello"`)
  - `:path` - HTTP/2 request path (e.g. `"/helloworld.Greeter/SayHello"`)
  - `:request_stream` - Boolean indicating whether the request is streamed
  - `:response_stream` - Boolean indicating whether the response is streamed
  - `:headers` - List of `{"header-name", "header-value"}` string tuples
  - `:metadata` - User-provided call metadata map or keyword list
  - `:private` - Map for private middleware state (e.g. `:trace_id`, `:auth_user`, etc.)
  - `:opts` - Keyword list of options passed to the call or plug
  - `:conn` - `%SimpleGrpc.Client.Conn{}` (client) or `%Plug.Conn{}` (server)
  - `:schema` - `%SimpleGrpc.Schema{}` struct if available
  """

  defstruct [
    :role,
    :service,
    :service_name,
    :rpc,
    :rpc_name,
    :path,
    request_stream: false,
    response_stream: false,
    headers: [],
    req_headers: [],
    metadata: %{},
    private: %{},
    opts: [],
    conn: nil,
    schema: nil
  ]

  @type t :: %__MODULE__{
          role: :client | :server | nil,
          service: String.t() | nil,
          service_name: String.t() | nil,
          rpc: atom() | String.t() | nil,
          rpc_name: String.t() | nil,
          path: String.t() | nil,
          request_stream: boolean(),
          response_stream: boolean(),
          headers: list({String.t(), String.t()}),
          req_headers: list({String.t(), String.t()}),
          metadata: map() | keyword(),
          private: map(),
          opts: keyword(),
          conn: any(),
          schema: SimpleGrpc.Schema.t() | nil
        }

  @doc """
  Adds or replaces a header in the context's header list.
  Header keys are automatically lowercased and normalized.
  """
  def put_header(%__MODULE__{headers: headers} = ctx, key, value) do
    key_str = normalize_header_key(key)
    val_str = to_string(value)

    new_headers =
      headers
      |> Enum.reject(fn {k, _v} -> String.downcase(k) == key_str end)
      |> Kernel.++([{key_str, val_str}])

    %{ctx | headers: new_headers, req_headers: new_headers}
  end

  @doc """
  Gets the value of a header by case-insensitive key name.
  """
  def get_header(%__MODULE__{headers: headers}, key, default \\ nil) do
    key_str = normalize_header_key(key)

    case Enum.find(headers, fn {k, _v} -> String.downcase(k) == key_str end) do
      {_k, v} -> v
      nil -> default
    end
  end

  @doc """
  Deletes all occurrences of a header by case-insensitive key name.
  """
  def delete_header(%__MODULE__{headers: headers} = ctx, key) do
    key_str = normalize_header_key(key)
    new_headers = Enum.reject(headers, fn {k, _v} -> String.downcase(k) == key_str end)
    %{ctx | headers: new_headers, req_headers: new_headers}
  end

  @doc """
  Stores a private middleware key/value pair in `ctx.private`.
  """
  def put_private(%__MODULE__{private: private} = ctx, key, value) do
    %{ctx | private: Map.put(private, key, value)}
  end

  @doc """
  Retrieves a private middleware value by key.
  """
  def get_private(%__MODULE__{private: private}, key, default \\ nil) do
    Map.get(private, key, default)
  end

  @doc """
  Updates a private middleware value.
  """
  def update_private(%__MODULE__{private: private} = ctx, key, default, fun) when is_function(fun, 1) do
    %{ctx | private: Map.update(private, key, default, fun)}
  end

  @doc """
  Puts a user-defined metadata key/value into `ctx.metadata`.
  """
  def put_metadata(%__MODULE__{metadata: metadata} = ctx, key, value) when is_map(metadata) do
    %{ctx | metadata: Map.put(metadata, key, value)}
  end

  def put_metadata(%__MODULE__{metadata: metadata} = ctx, key, value) when is_list(metadata) do
    %{ctx | metadata: Keyword.put(metadata, key, value)}
  end

  @doc """
  Gets a user-defined metadata value by key.
  """
  def get_metadata(ctx, key, default \\ nil)

  def get_metadata(%__MODULE__{metadata: metadata}, key, default) when is_map(metadata) do
    Map.get(metadata, key, default)
  end

  def get_metadata(%__MODULE__{metadata: metadata}, key, default) when is_list(metadata) do
    Keyword.get(metadata, key, default)
  end

  def get_metadata(_ctx, _key, default), do: default

  defp normalize_header_key(key) when is_atom(key) do
    key |> to_string() |> String.replace("_", "-") |> String.downcase()
  end

  defp normalize_header_key(key) when is_binary(key) do
    String.downcase(key)
  end

  defp normalize_header_key(key), do: key |> to_string() |> String.downcase()
end
