import Bitwise

defmodule SimpleGrpc.Schema.Field do
  @moduledoc """
  Represents a field, map, or oneof within a protobuf message definition.
  """
  import Bitwise

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
    :tag_bytes,
    :wire_type,
    :is_packable,
    :oneof_name,
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
          tag_bytes: binary() | nil,
          wire_type: integer() | nil,
          is_packable: boolean() | nil,
          oneof_name: atom() | nil,
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

    wire_type = compute_wire_type(type)
    tag_bytes = if wire_type && is_integer(number), do: encode_tag(number, wire_type), else: nil
    is_packable = compute_is_packable(type)

    %__MODULE__{
      kind: :field,
      name: String.to_atom(name),
      type: type,
      number: number,
      prefix: prefix,
      deprecated: deprecated,
      options: options,
      wire_type: wire_type,
      tag_bytes: tag_bytes,
      is_packable: is_packable
    }
  end

  def parse({:map, [key_type, val_type, name, number | rest]}) do
    options =
      Enum.reduce(rest, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    tag_bytes = if is_integer(number), do: encode_tag(number, 2), else: nil

    %__MODULE__{
      kind: :map,
      name: String.to_atom(name),
      key_type: key_type,
      val_type: val_type,
      type: {:map, key_type, val_type},
      number: number,
      prefix: nil,
      deprecated: false,
      options: options,
      wire_type: 2,
      tag_bytes: tag_bytes,
      is_packable: false
    }
  end

  def parse({:oneof, [oneof_name | fields]}) do
    oneof_atom = String.to_atom(oneof_name)

    parsed_fields =
      fields
      |> Enum.filter(fn
        {:field, _} -> true
        _ -> false
      end)
      |> Enum.map(&parse/1)
      |> Enum.map(fn f -> %{f | oneof_name: oneof_atom} end)

    %__MODULE__{
      kind: :oneof,
      name: oneof_atom,
      fields: parsed_fields,
      deprecated: false,
      oneof_name: oneof_atom
    }
  end

  def compute_wire_type("int32"), do: 0
  def compute_wire_type("int64"), do: 0
  def compute_wire_type("uint32"), do: 0
  def compute_wire_type("uint64"), do: 0
  def compute_wire_type("sint32"), do: 0
  def compute_wire_type("sint64"), do: 0
  def compute_wire_type("bool"), do: 0
  def compute_wire_type("fixed64"), do: 1
  def compute_wire_type("sfixed64"), do: 1
  def compute_wire_type("double"), do: 1
  def compute_wire_type("string"), do: 2
  def compute_wire_type("bytes"), do: 2
  def compute_wire_type({:map, _, _}), do: 2
  def compute_wire_type("fixed32"), do: 5
  def compute_wire_type("sfixed32"), do: 5
  def compute_wire_type("float"), do: 5
  def compute_wire_type(_other), do: nil

  def compute_is_packable("int32"), do: true
  def compute_is_packable("int64"), do: true
  def compute_is_packable("uint32"), do: true
  def compute_is_packable("uint64"), do: true
  def compute_is_packable("sint32"), do: true
  def compute_is_packable("sint64"), do: true
  def compute_is_packable("bool"), do: true
  def compute_is_packable("fixed64"), do: true
  def compute_is_packable("sfixed64"), do: true
  def compute_is_packable("double"), do: true
  def compute_is_packable("fixed32"), do: true
  def compute_is_packable("sfixed32"), do: true
  def compute_is_packable("float"), do: true
  def compute_is_packable(_other), do: false

  defp encode_tag(field_num, wire_type) do
    tag = (field_num <<< 3) ||| wire_type
    cond do
      tag < 128 ->
        <<tag>>

      tag < 16384 ->
        <<1::1, (tag &&& 0x7F)::7, 0::1, (tag >>> 7)::7>>

      tag < 2_097_152 ->
        <<1::1, (tag &&& 0x7F)::7, 1::1, ((tag >>> 7) &&& 0x7F)::7, 0::1, (tag >>> 14)::7>>

      true ->
        <<1::1, (tag &&& 0x7F)::7, 1::1, ((tag >>> 7) &&& 0x7F)::7, 1::1,
          ((tag >>> 14) &&& 0x7F)::7, 0::1, (tag >>> 21)::7>>
    end
  end
end
