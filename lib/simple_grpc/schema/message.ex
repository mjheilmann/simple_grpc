defmodule SimpleGrpc.Schema.Message do
  @moduledoc """
  Represents a parsed protobuf message descriptor.
  """

  alias SimpleGrpc.Schema.Field
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum

  defstruct [
    :name,
    :full_name,
    fields: [],
    fields_by_number: %{},
    fields_by_name: %{},
    nested_messages: [],
    nested_enums: [],
    has_oneofs: false,
    default_map: nil,
    repeated_fields: [],
    options: %{}
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          full_name: String.t() | nil,
          fields: list(Field.t()),
          fields_by_number: %{integer() => Field.t()},
          fields_by_name: %{atom() => Field.t()},
          nested_messages: list(t()),
          nested_enums: list(SchemaEnum.t() | map()),
          has_oneofs: boolean(),
          default_map: map() | nil,
          repeated_fields: list(atom()),
          options: map()
        }

  @doc """
  Parses a message AST node into a Message descriptor struct.
  """
  def parse({:message, items}) when is_list(items) do
    parse(items)
  end

  def parse([name | items]) when is_binary(name) do
    fields =
      items
      |> Enum.flat_map(fn
        {:field, _} = f -> [Field.parse(f)]
        {:map, _} = m -> [Field.parse(m)]
        {:oneof, _} = o -> [Field.parse(o)]
        _ -> []
      end)

    nested_messages =
      items
      |> Enum.filter(fn
        {:message, _} -> true
        _ -> false
      end)
      |> Enum.map(&parse/1)

    nested_enums =
      items
      |> Enum.filter(fn
        {:enum, _} -> true
        _ -> false
      end)
      |> Enum.map(&SchemaEnum.parse/1)

    options =
      Enum.reduce(items, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    fields_by_number =
      fields
      |> Enum.flat_map(fn
        %Field{kind: :oneof, fields: oneof_fields} ->
          Enum.map(oneof_fields, &{&1.number, &1})

        %Field{number: num} = field when is_integer(num) ->
          [{num, field}]

        _ ->
          []
      end)
      |> Map.new()

    fields_by_name =
      fields
      |> Enum.flat_map(fn
        %Field{kind: :oneof, fields: oneof_fields} ->
          Enum.map(oneof_fields, &{&1.name, &1})

        %Field{name: fname} = field when is_atom(fname) ->
          [{fname, field}]

        _ ->
          []
      end)
      |> Map.new()

    has_oneofs = Enum.any?(fields, fn f -> f.kind == :oneof end)

    default_map =
      Enum.reduce(fields, %{}, fn field, acc ->
        case field.kind do
          :oneof ->
            Enum.reduce(field.fields, acc, fn subfield, inner ->
              Map.put(inner, subfield.name, nil)
            end)

          :map ->
            Map.put(acc, field.name, %{})

          _ ->
            if field.prefix == :repeated do
              Map.put(acc, field.name, [])
            else
              if field.prefix == :optional do
                Map.put(acc, field.name, nil)
              else
                Map.put(acc, field.name, field.default_value)
              end
            end
        end
      end)

    repeated_fields =
      fields
      |> Enum.filter(fn f -> f.prefix == :repeated end)
      |> Enum.map(& &1.name)

    %__MODULE__{
      name: name,
      full_name: name,
      fields: fields,
      fields_by_number: fields_by_number,
      fields_by_name: fields_by_name,
      nested_messages: nested_messages,
      nested_enums: nested_enums,
      has_oneofs: has_oneofs,
      default_map: default_map,
      repeated_fields: repeated_fields,
      options: options
    }
  end

  @doc """
  Finds a field by field number.
  """
  def field_by_number(%__MODULE__{fields_by_number: map}, number) when is_integer(number) do
    Map.get(map, number)
  end

  @doc """
  Finds a field by atom or string name.
  """
  def field_by_name(%__MODULE__{fields_by_name: map}, name) when is_atom(name) do
    Map.get(map, name)
  end

  def field_by_name(%__MODULE__{} = msg, name) when is_binary(name) do
    field_by_name(msg, String.to_atom(name))
  end

  @doc """
  Extracts struct field atom names for `defstruct`.
  """
  def struct_field_names(%__MODULE__{fields: fields}) do
    Enum.flat_map(fields, fn
      %Field{kind: :field, name: name} ->
        [name]

      %Field{kind: :map, name: name} ->
        [name]

      %Field{kind: :oneof, fields: oneof_fields} ->
        Enum.map(oneof_fields, & &1.name)
    end)
    |> Enum.uniq()
  end
end
