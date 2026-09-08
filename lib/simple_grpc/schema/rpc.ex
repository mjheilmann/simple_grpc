defmodule SimpleGrpc.Schema.RPC do
  @moduledoc """
  Represents a parsed RPC method in a gRPC service definition.
  """

  defstruct [
    :name,
    :path,
    :input,
    :output,
    request_stream: false,
    response_stream: false,
    options: []
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          path: String.t() | nil,
          input: String.t(),
          output: String.t(),
          request_stream: boolean(),
          response_stream: boolean(),
          options: list()
        }

  @doc """
  Parses an RPC definition from the parser AST into an RPC struct.
  """
  def parse({:rpc, items}) when is_list(items) do
    parse(items)
  end

  def parse([name | rest]) when is_binary(name) do
    {req_stream, rest} =
      case rest do
        [:stream, req_type | rest] -> {true, [req_type | rest]}
        [req_type | rest] -> {false, [req_type | rest]}
      end

    [req_type | rest] = rest

    {resp_stream, rest} =
      case rest do
        [:stream, resp_type | rest] -> {true, [resp_type | rest]}
        [resp_type | rest] -> {false, [resp_type | rest]}
      end

    [resp_type | rest] = rest

    options =
      Enum.filter(rest, fn
        {:option, _} -> true
        _ -> false
      end)

    %__MODULE__{
      name: name,
      input: req_type,
      output: resp_type,
      request_stream: req_stream,
      response_stream: resp_stream,
      options: options
    }
  end
end
