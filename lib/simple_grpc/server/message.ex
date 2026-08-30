defmodule SimpleGrpc.Server.Message do
  @moduledoc """
  Represents a parsed protobuf message and generates the corresponding Elixir module AST.
  """

  alias SimpleGrpc.Server.Field

  defstruct [
    :name,
    fields: [],
    nested_messages: [],
    nested_enums: [],
    options: %{}
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          fields: list(Field.t()),
          nested_messages: list(t()),
          nested_enums: list(map()),
          options: map()
        }

  @doc """
  Parses a message AST node from the proto3 parser.
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
      |> Enum.map(fn {:enum, [enum_name | enum_items]} ->
        enum_fields =
          Enum.flat_map(enum_items, fn
            {:enum_field, [fname, fval | _]} -> [{String.to_atom(fname), fval}]
            _ -> []
          end)

        %{name: enum_name, fields: enum_fields}
      end)

    options =
      Enum.reduce(items, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    %__MODULE__{
      name: name,
      fields: fields,
      nested_messages: nested_messages,
      nested_enums: nested_enums,
      options: options
    }
  end

  @doc """
  Extracts struct field atom names for `defstruct`.
  """
  def struct_field_names(%__MODULE__{fields: fields}) do
    Enum.flat_map(fields, fn
      %Field{kind: :field, name: name} -> [name]
      %Field{kind: :map, name: name} -> [name]
      %Field{kind: :oneof, fields: oneof_fields} ->
        Enum.map(oneof_fields, & &1.name)
    end)
    |> Enum.uniq()
  end

  @doc """
  Generates the AST for a message module and all of its nested message submodules.
  """
  def generate_module_ast(%__MODULE__{} = message, parent_module) do
    mod_name = Module.concat(parent_module, message.name)
    field_atoms = struct_field_names(message)
    fields_escaped = Macro.escape(message.fields)
    enums_escaped = Macro.escape(message.nested_enums)
    message_name = message.name

    nested_submodules_ast =
      Enum.map(message.nested_messages, fn nested_msg ->
        generate_module_ast(nested_msg, mod_name)
      end)

    quote do
      defmodule unquote(mod_name) do
        @moduledoc """
        Protobuf message struct for `#{unquote(message_name)}`.
        """
        defstruct unquote(field_atoms)

        def new(attrs \\ %{}) do
          SimpleGrpc.Server.Coercer.coerce(attrs, __MODULE__)
        end

        def __message_name__, do: unquote(message_name)

        def __fields__, do: unquote(fields_escaped)

        def __field__(field_name) when is_atom(field_name) do
          Enum.find(__fields__(), fn
            %SimpleGrpc.Server.Field{name: ^field_name} -> true
            %SimpleGrpc.Server.Field{kind: :oneof, fields: oneof_fields} ->
              Enum.any?(oneof_fields, &(&1.name == field_name))
            _ -> false
          end)
        end

        def __field__(field_name) when is_binary(field_name) do
          __field__(String.to_atom(field_name))
        end

        def __nested_enums__, do: unquote(enums_escaped)

        unquote_splicing(nested_submodules_ast)
      end
    end
  end
end
