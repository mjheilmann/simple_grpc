defmodule SimpleGrpc.Coercer do
  @moduledoc """
  Coerces and validates input/output attributes (maps, keyword lists, structs)
  against protobuf message definitions, applying proto3 default values and resolving enums.
  """

  alias SimpleGrpc.Schema.Field

  @doc """
  Coerces `attrs` into a struct of `message_module`, validating fields and applying proto3 defaults.
  """
  def coerce(attrs, message_module) when is_atom(message_module) do
    fields = message_module.__fields__()
    raw_map = to_plain_map(attrs)

    valid_field_atoms =
      Enum.flat_map(fields, fn
        %Field{kind: :field, name: name} -> [name]
        %Field{kind: :map, name: name} -> [name]
        %Field{kind: :oneof, fields: oneof_fields} -> Enum.map(oneof_fields, & &1.name)
      end)
      |> MapSet.new()

    # Check for unrecognized keys
    given_keys =
      raw_map
      |> Map.keys()
      |> Enum.reject(&(&1 in [:__struct__, :__unknown_fields__]))
      |> MapSet.new()

    unknown_keys = MapSet.difference(given_keys, valid_field_atoms)

    if MapSet.size(unknown_keys) > 0 do
      unknown_list = unknown_keys |> MapSet.to_list() |> Enum.map_join(", ", &inspect/1)
      msg_name = message_module.__message_name__()

      raise ArgumentError,
            "Unknown field(s) [#{unknown_list}] provided for protobuf message #{inspect(msg_name)}"
    end

    coerced_data =
      Enum.reduce(fields, %{}, fn field, acc ->
        case field.kind do
          :oneof ->
            # Populate any provided oneof field, otherwise leave as nil
            Enum.reduce(field.fields, acc, fn oneof_subfield, inner_acc ->
              val = Map.get(raw_map, oneof_subfield.name)

              if is_nil(val) do
                inner_acc
              else
                Map.put(
                  inner_acc,
                  oneof_subfield.name,
                  coerce_field_value(val, oneof_subfield, message_module)
                )
              end
            end)

          _ ->
            val = Map.get(raw_map, field.name)

            coerced_val =
              if is_nil(val) do
                default_value(field, message_module)
              else
                coerce_field_value(val, field, message_module)
              end

            Map.put(acc, field.name, coerced_val)
        end
      end)

    struct(message_module, Map.merge(coerced_data, unknown_fields(raw_map)))
  end

  # Fields received on the wire that the schema does not know are carried along.
  defp unknown_fields(%{__unknown_fields__: unknown}), do: %{__unknown_fields__: unknown}
  defp unknown_fields(_raw_map), do: %{}

  defp to_plain_map(%{__struct__: _} = s), do: Map.from_struct(s)

  defp to_plain_map(map) when is_map(map) do
    Enum.reduce(map, %{}, fn {k, v}, acc ->
      atom_key = if is_binary(k), do: String.to_atom(k), else: k
      Map.put(acc, atom_key, v)
    end)
  end

  defp to_plain_map(kw) when is_list(kw) do
    Enum.reduce(kw, %{}, fn {k, v}, acc ->
      atom_key = if is_binary(k), do: String.to_atom(k), else: k
      Map.put(acc, atom_key, v)
    end)
  end

  defp default_value(%Field{prefix: :optional}, _mod), do: nil
  defp default_value(%Field{prefix: :repeated}, _mod), do: []
  defp default_value(%Field{kind: :map}, _mod), do: %{}
  defp default_value(%Field{type: "string"}, _mod), do: ""
  defp default_value(%Field{type: "bytes"}, _mod), do: <<>>
  defp default_value(%Field{type: "bool"}, _mod), do: false

  defp default_value(%Field{type: t}, _mod)
       when t in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64),
       do: 0

  defp default_value(%Field{type: t}, _mod) when t in ~w(float double), do: 0.0

  defp default_value(%Field{type: type}, message_module) do
    {schema_ctx, msg_desc} = enum_resolution_ctx(message_module)

    if SimpleGrpc.Protobuf.is_enum_type?(type, schema_ctx, msg_desc) do
      enum_desc = SimpleGrpc.Protobuf.resolve_enum_descriptor(type, schema_ctx, msg_desc)

      if enum_desc do
        Enum.find_value(enum_desc.fields, 0, fn {k, v} -> if v == 0, do: k end)
      else
        0
      end
    else
      nil
    end
  end

  defp coerce_field_value(val, %Field{prefix: :repeated}, _mod) when is_list(val), do: val

  defp coerce_field_value(val, %Field{prefix: :repeated}, _mod) do
    raise ArgumentError, "Expected list for repeated field, got: #{inspect(val)}"
  end

  defp coerce_field_value(val, %Field{kind: :map}, _mod) when is_map(val), do: val

  defp coerce_field_value(val, %Field{kind: :map}, _mod) when is_list(val),
    do: Enum.into(val, %{})

  defp coerce_field_value(val, %Field{type: "string"}, _mod) when is_binary(val), do: val

  defp coerce_field_value(val, %Field{type: "string"}, _mod) when is_atom(val),
    do: Atom.to_string(val)

  defp coerce_field_value(val, %Field{type: "bytes"}, _mod) when is_binary(val), do: val
  defp coerce_field_value(val, %Field{type: "bool"}, _mod) when is_boolean(val), do: val

  defp coerce_field_value(val, %Field{type: t}, _mod)
       when t in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64) and
              is_integer(val),
       do: val

  defp coerce_field_value(val, %Field{type: t}, _mod)
       when t in ~w(float double) and is_number(val),
       do: val * 1.0

  defp coerce_field_value(val, %Field{type: type}, message_module)
       when is_atom(val) or is_binary(val) or is_integer(val) do
    {schema_ctx, msg_desc} = enum_resolution_ctx(message_module)

    if SimpleGrpc.Protobuf.is_enum_type?(type, schema_ctx, msg_desc) do
      enum_desc = SimpleGrpc.Protobuf.resolve_enum_descriptor(type, schema_ctx, msg_desc)

      if enum_desc do
        cond do
          is_atom(val) ->
            case Enum.find(enum_desc.fields, fn {k, _v} ->
                   k == val or
                     String.upcase(Atom.to_string(k)) == String.upcase(Atom.to_string(val))
                 end) do
              {k, _} -> k
              nil -> val
            end

          is_binary(val) ->
            case Enum.find(enum_desc.fields, fn {k, _v} ->
                   Atom.to_string(k) == val or
                     String.upcase(Atom.to_string(k)) == String.upcase(val)
                 end) do
              {k, _} -> k
              nil -> val
            end

          is_integer(val) ->
            case Enum.find(enum_desc.fields, fn {_k, v} -> v == val end) do
              {k, _} -> k
              nil -> val
            end
        end
      else
        val
      end
    else
      val
    end
  end

  defp coerce_field_value(val, _field, _mod), do: val

  # Returns {schema_ctx, msg_desc} for enum resolution.
  # Prefers the module's schema (from __schema__/0) as primary context,
  # and synthesises a %Message{} from __nested_enums__/0 as a fallback msg_desc.
  defp enum_resolution_ctx(nil), do: {nil, nil}

  defp enum_resolution_ctx(mod) when is_atom(mod) do
    schema_ctx =
      if function_exported?(mod, :__schema__, 0) do
        mod.__schema__()
      else
        nil
      end

    msg_desc =
      if function_exported?(mod, :__nested_enums__, 0) and
           function_exported?(mod, :__message_name__, 0) do
        %SimpleGrpc.Schema.Message{
          name: mod.__message_name__(),
          fields: [],
          nested_enums: mod.__nested_enums__()
        }
      else
        nil
      end

    {schema_ctx, msg_desc}
  end

  defp enum_resolution_ctx(_other), do: {nil, nil}
end
