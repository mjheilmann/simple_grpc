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

  The module is `parent_module.<message name>`. `schema_module` is the module whose
  `__schema__/0` describes the message; it defaults to `parent_module`.
  """
  def generate_module_ast(%Message{} = message, parent_module, schema_module \\ nil) do
    schema_module = schema_module || parent_module
    mod_name = Module.concat(parent_module, message.name)
    body = struct_body_ast(message, mod_name, schema_module)

    quote do
      defmodule unquote(mod_name) do
        @moduledoc """
        Protobuf message struct for `#{unquote(message.name)}`.
        """

        unquote(body)
      end
    end
  end

  @doc """
  Defines the struct of a message that ships with the library (a bundled well-known type)
  inside the calling module, so the module can add its own helpers next to it.

  `full_name` is the message's fully-qualified name in the bundled schemas. The module's
  `__schema__/0` is answered by `schema_module`, which defaults to `SimpleGrpc.WKT`.
  """
  defmacro define_bundled_struct(full_name, schema_module \\ SimpleGrpc.WKT) do
    schema_module = Macro.expand(schema_module, __CALLER__)

    message =
      SimpleGrpc.Schema.WellKnownTypes.unified_schema().messages[full_name] ||
        raise ArgumentError, "#{full_name} is not a bundled message"

    struct_body_ast(message, __CALLER__.module, schema_module)
  end

  @doc false
  # The contents of a message module (everything except `@moduledoc`), including the
  # modules of nested messages, which are named under `mod_name`.
  def struct_body_ast(%Message{} = message, mod_name, schema_module) do
    field_atoms = struct_field_names(message)
    fields_escaped = Macro.escape(message.fields)
    enums_escaped = Macro.escape(message.nested_enums)
    message_name = message.name

    nested_submodules_ast =
      Enum.map(message.nested_messages, fn nested_msg ->
        generate_module_ast(nested_msg, mod_name, schema_module)
      end)

    field_type_specs = build_field_types(message.fields, mod_name)
    struct_type_spec = {:%, [], [{:__MODULE__, [], Elixir}, {:%{}, [], field_type_specs}]}

    quote do
      @type t :: unquote(struct_type_spec)
      @type attrs :: %{optional(atom() | String.t()) => any()} | keyword()

      defstruct unquote(field_atoms ++ [__unknown_fields__: []])

      @spec new(attrs :: attrs()) :: t()
      def new(attrs \\ %{}) do
        SimpleGrpc.Coercer.coerce(attrs, __MODULE__)
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

      def __schema__ do
        if Code.ensure_loaded?(unquote(schema_module)) and
             function_exported?(unquote(schema_module), :__schema__, 0) do
          apply(unquote(schema_module), :__schema__, [])
        else
          nil
        end
      end

      unquote_splicing(nested_submodules_ast)
    end
  end

  defp build_field_types(fields, parent_module) when is_list(fields) do
    Enum.flat_map(fields, fn
      %SimpleGrpc.Schema.Field{kind: :oneof, fields: subfields} ->
        Enum.map(subfields, fn sf ->
          {sf.name, build_field_type(sf, parent_module)}
        end)

      %SimpleGrpc.Schema.Field{name: name} = f ->
        [{name, build_field_type(f, parent_module)}]

      _ ->
        []
    end)
    |> Enum.uniq_by(fn {k, _v} -> k end)
  end

  defp build_field_type(
         %SimpleGrpc.Schema.Field{kind: :map, key_type: k_type, val_type: v_type},
         parent_module
       ) do
    k_ast = primitive_or_submodule_type(k_type, parent_module)
    v_ast = primitive_or_submodule_type(v_type, parent_module)
    quote do: %{optional(unquote(k_ast)) => unquote(v_ast)}
  end

  defp build_field_type(
         %SimpleGrpc.Schema.Field{kind: :field, prefix: :repeated, type: type},
         parent_module
       ) do
    elem_ast = primitive_or_submodule_type(type, parent_module)
    quote do: [unquote(elem_ast)]
  end

  defp build_field_type(%SimpleGrpc.Schema.Field{kind: :field, type: type}, parent_module) do
    elem_ast = primitive_or_submodule_type(type, parent_module)
    quote do: unquote(elem_ast) | nil
  end

  defp build_field_type(%SimpleGrpc.Schema.Field{type: type}, parent_module) do
    elem_ast = primitive_or_submodule_type(type, parent_module)
    quote do: unquote(elem_ast) | nil
  end

  defp primitive_or_submodule_type(type, _parent_module) do
    case type do
      "string" ->
        quote do: String.t()

      "bytes" ->
        quote do: binary()

      "bool" ->
        quote do: boolean()

      t
      when t in [
             "int32",
             "int64",
             "uint32",
             "uint64",
             "sint32",
             "sint64",
             "fixed32",
             "fixed64",
             "sfixed32",
             "sfixed64"
           ] ->
        quote do: integer()

      t when t in ["float", "double"] ->
        quote do: float() | integer()

      _other ->
        quote do: any()
    end
  end
end
