defmodule SimpleGrpc.Schema.Enum do
  @moduledoc """
  Represents a protobuf enum definition.
  """

  defstruct [
    :name,
    fields: [],
    options: %{}
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          fields: list({atom(), integer()}),
          options: map()
        }

  @doc """
  Parses an enum AST node into an Enum struct.
  """
  def parse({:enum, [name | items]}) when is_binary(name) do
    fields =
      Enum.flat_map(items, fn
        {:enum_field, [fname, fval | _]} -> [{String.to_atom(fname), fval}]
        _ -> []
      end)

    options =
      Enum.reduce(items, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    %__MODULE__{
      name: name,
      fields: fields,
      options: options
    }
  end
end
