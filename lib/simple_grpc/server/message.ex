defmodule SimpleGrpc.Server.Message do
  @moduledoc """
  Represents a parsed protobuf message and generates the corresponding Elixir module AST.
  """

  alias SimpleGrpc.Schema.Message

  defstruct [
    :name,
    :full_name,
    fields: [],
    fields_by_number: %{},
    fields_by_name: %{},
    nested_messages: [],
    nested_enums: [],
    options: %{}
  ]

  @type t :: Message.t()

  defdelegate parse(ast), to: Message
  defdelegate struct_field_names(msg), to: Message

  @doc """
  Generates the AST for a message module and all of its nested message submodules.
  """
  def generate_module_ast(%Message{} = message, parent_module) do
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

        def __field_by_number__(number) when is_integer(number) do
          Enum.find(__fields__(), fn
            %SimpleGrpc.Schema.Field{number: ^number} ->
              true

            %SimpleGrpc.Schema.Field{kind: :oneof, fields: oneof_fields} ->
              Enum.any?(oneof_fields, &(&1.number == number))

            _ ->
              false
          end)
        end

        def __field__(field_name) when is_atom(field_name) do
          Enum.find(__fields__(), fn
            %{name: ^field_name} ->
              true

            %{kind: :oneof, fields: oneof_fields} ->
              Enum.any?(oneof_fields, &(&1.name == field_name))

            _ ->
              false
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
