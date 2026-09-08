defmodule SimpleGrpc.Schema.Field do
  @moduledoc """
  Represents a field, map, or oneof within a protobuf message definition.
  """

  defstruct [
    :name,
    :type,
    :number,
    :prefix,
    :deprecated,
    :kind,
    :key_type,
    :val_type,
    :fields,
    options: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          type: any(),
          number: integer() | nil,
          prefix: atom() | nil,
          deprecated: boolean(),
          kind: :field | :map | :oneof,
          key_type: any(),
          val_type: any(),
          fields: list(t()) | nil,
          options: map()
        }

  @doc """
  Parses a field, map, or oneof AST node into a Field struct.
  """
  def parse({:field, items}) when is_list(items) do
    {prefix, items} =
      case items do
        [{:prefix, prefix} | rest] -> {prefix, rest}
        rest -> {nil, rest}
      end

    [type, name, number | rest] = items

    deprecated =
      Enum.find_value(rest, false, fn
        {:deprecated, [val]} -> val
        {:deprecated, val} -> val
        _ -> false
      end)

    options =
      Enum.reduce(rest, %{}, fn
        {:deprecated, [val]}, acc -> Map.put(acc, :deprecated, val)
        {:deprecated, val}, acc -> Map.put(acc, :deprecated, val)
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    %__MODULE__{
      kind: :field,
      name: String.to_atom(name),
      type: type,
      number: number,
      prefix: prefix,
      deprecated: deprecated,
      options: options
    }
  end

  def parse({:map, [key_type, val_type, name, number | rest]}) do
    options =
      Enum.reduce(rest, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    %__MODULE__{
      kind: :map,
      name: String.to_atom(name),
      key_type: key_type,
      val_type: val_type,
      type: {:map, key_type, val_type},
      number: number,
      prefix: nil,
      deprecated: false,
      options: options
    }
  end

  def parse({:oneof, [oneof_name | fields]}) do
    parsed_fields =
      fields
      |> Enum.filter(fn
        {:field, _} -> true
        _ -> false
      end)
      |> Enum.map(&parse/1)

    %__MODULE__{
      kind: :oneof,
      name: String.to_atom(oneof_name),
      fields: parsed_fields,
      deprecated: false
    }
  end
end
