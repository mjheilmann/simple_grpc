defmodule SimpleGrpc.Server.Coercer do
  @moduledoc """
  Coerces and validates input/output attributes (maps, keyword lists, structs)
  against protobuf message definitions, applying proto3 default values.
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
      |> Enum.reject(&(&1 in [:__struct__]))
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
                Map.put(inner_acc, oneof_subfield.name, coerce_field_value(val, oneof_subfield))
              end
            end)

          _ ->
            val = Map.get(raw_map, field.name)

            coerced_val =
              if is_nil(val) do
                default_value(field)
              else
                coerce_field_value(val, field)
              end

            Map.put(acc, field.name, coerced_val)
        end
      end)

    struct(message_module, coerced_data)
  end

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

  defp default_value(%Field{prefix: :optional}), do: nil
  defp default_value(%Field{prefix: :repeated}), do: []
  defp default_value(%Field{kind: :map}), do: %{}
  defp default_value(%Field{type: "string"}), do: ""
  defp default_value(%Field{type: "bytes"}), do: <<>>
  defp default_value(%Field{type: "bool"}), do: false

  defp default_value(%Field{type: t})
       when t in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64),
       do: 0

  defp default_value(%Field{type: t}) when t in ~w(float double), do: 0.0
  defp default_value(_), do: nil

  defp coerce_field_value(val, %Field{prefix: :repeated}) when is_list(val), do: val

  defp coerce_field_value(val, %Field{prefix: :repeated}) do
    raise ArgumentError, "Expected list for repeated field, got: #{inspect(val)}"
  end

  defp coerce_field_value(val, %Field{kind: :map}) when is_map(val), do: val
  defp coerce_field_value(val, %Field{kind: :map}) when is_list(val), do: Enum.into(val, %{})
  defp coerce_field_value(val, %Field{type: "string"}) when is_binary(val), do: val
  defp coerce_field_value(val, %Field{type: "string"}) when is_atom(val), do: Atom.to_string(val)
  defp coerce_field_value(val, %Field{type: "bytes"}) when is_binary(val), do: val
  defp coerce_field_value(val, %Field{type: "bool"}) when is_boolean(val), do: val

  defp coerce_field_value(val, %Field{type: t})
       when t in ~w(int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64) and
              is_integer(val), do: val

  defp coerce_field_value(val, %Field{type: t}) when t in ~w(float double) and is_number(val),
    do: val * 1.0

  defp coerce_field_value(val, _field), do: val
end
