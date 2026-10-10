defmodule SimpleGrpc.Compiler do
  @moduledoc """
  Generates and compiles high-performance BEAM bytecode encoder/decoder modules
  for `SimpleGrpc.Schema` definitions.

  Operates directly on plain Elixir maps, structs, and keyword lists with
  zero external dependencies or pre-generation steps.
  """

  import Bitwise

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.{Message, Field}
  alias SimpleGrpc.Schema.Enum, as: SchemaEnum

  @doc """
  Compiles a `SimpleGrpc.Schema` into an in-memory BEAM module and sets `:compiled_module`
  on the schema struct.
  """
  def compile_schema(%Schema{messages: messages} = schema) when map_size(messages) == 0 do
    schema
  end

  def compile_schema(%Schema{compiled_module: mod} = schema) when not is_nil(mod) do
    schema
  end

  def compile_schema(%Schema{} = schema) do
    module_name = generate_module_name(schema)

    # Generate AST for module
    module_ast = generate_module_ast(module_name, schema)

    # Compile into BEAM VM
    [{^module_name, _binary}] = Code.compile_quoted(module_ast)

    %{schema | compiled_module: module_name}
  catch
    kind, reason ->
      # If JIT compilation encounters any unsupported construct, safely fall back to dynamic
      require Logger

      Logger.debug(
        "[SimpleGrpc.Compiler] Schema compilation falling back to dynamic: #{inspect(kind)} #{inspect(reason)}"
      )

      schema
  end

  defp generate_module_name(%Schema{package: package}) do
    uniq = System.unique_integer([:positive])

    pkg_part =
      if package && package != "",
        do: Macro.camelize(String.replace(package, ".", "_")),
        else: "Root"

    Module.concat([SimpleGrpc.Compiled, :"#{pkg_part}_#{uniq}"])
  end

  defp generate_module_ast(module_name, %Schema{messages: messages} = schema) do
    decode_dispatch_clauses =
      Enum.flat_map(messages, fn {name, msg} ->
        fun_name = msg_fun_name(:decode, name)
        short_name = msg.name

        clauses = [
          quote do
            def decode(unquote(name), binary), do: unquote(fun_name)(binary)
          end
        ]

        if short_name != name do
          clauses ++
            [
              quote do
                def decode(unquote(short_name), binary), do: unquote(fun_name)(binary)
              end
            ]
        else
          clauses
        end
      end)

    encode_dispatch_clauses =
      Enum.flat_map(messages, fn {name, msg} ->
        fun_name = msg_fun_name(:encode, name)
        short_name = msg.name

        clauses = [
          quote do
            def encode(unquote(name), data), do: unquote(fun_name)(data)
          end
        ]

        if short_name != name do
          clauses ++
            [
              quote do
                def encode(unquote(short_name), data), do: unquote(fun_name)(data)
              end
            ]
        else
          clauses
        end
      end)

    msg_code =
      Enum.map(messages, fn {name, msg} ->
        generate_message_code(name, msg, schema)
      end)

    quote do
      defmodule unquote(module_name) do
        @moduledoc false
        import Bitwise

        unquote_splicing(decode_dispatch_clauses)
        def decode(_name, _binary), do: :error

        unquote_splicing(encode_dispatch_clauses)
        def encode(_name, _data), do: :error

        unquote_splicing(msg_code)
      end
    end
  end

  defp generate_message_code(msg_full_name, %Message{fields: fields} = msg, schema) do
    decode_fn = msg_fun_name(:decode, msg_full_name)
    decode_loop_fn = msg_fun_name(:decode_loop, msg_full_name)
    encode_fn = msg_fun_name(:encode, msg_full_name)

    default_map = msg.default_map || %{}
    repeated_fields = msg.repeated_fields || []
    required_fields = msg.required_fields || []

    req_validation =
      Enum.map(required_fields, fn req_f ->
        quote do
          if is_nil(Map.get(final_acc, unquote(req_f))) do
            throw({:error, unquote("missing required field '#{req_f}'")})
          end
        end
      end)

    # Terminal decode loop clause: binary exhausted -> finalize repeated fields & validate required
    terminal_clause =
      case repeated_fields do
        [] ->
          if req_validation == [] do
            quote do
              defp unquote(decode_loop_fn)(<<>>, acc), do: acc
            end
          else
            quote do
              defp unquote(decode_loop_fn)(<<>>, acc) do
                final_acc = acc
                unquote_splicing(req_validation)
                final_acc
              end
            end
          end

        [single_rep] ->
          quote do
            defp unquote(decode_loop_fn)(<<>>, acc) do
              final_acc = %{acc | unquote(single_rep) => :lists.reverse(acc.unquote(single_rep))}
              unquote_splicing(req_validation)
              final_acc
            end
          end

        multiple_rep ->
          reversals =
            Enum.map(multiple_rep, fn f ->
              {f, quote(do: :lists.reverse(acc.unquote(f)))}
            end)

          quote do
            defp unquote(decode_loop_fn)(<<>>, acc) do
              final_acc = %{acc | unquote_splicing(reversals)}
              unquote_splicing(req_validation)
              final_acc
            end
          end
      end

    # Generate decode clauses for fields
    field_decode_clauses =
      Enum.flat_map(fields, fn field ->
        generate_field_decode_clauses(decode_loop_fn, field, schema)
      end)

    # Fallback decode clause for unknown fields
    unknown_field_clauses = [
      quote do
        defp unquote(decode_loop_fn)(binary, acc) when is_binary(binary) do
          orig_len = byte_size(binary)
          {tag, rest1} = SimpleGrpc.Protobuf.decode_varint(binary)
          wire_type = tag &&& 7

          case SimpleGrpc.Protobuf.skip_wire_value(rest1, wire_type) do
            {:ok, rest2} ->
              consumed_len = orig_len - byte_size(rest2)
              unknown_wire = :binary.part(binary, 0, consumed_len)
              unknowns = Map.get(acc, :__unknown_fields__, []) ++ [unknown_wire]
              unquote(decode_loop_fn)(rest2, Map.put(acc, :__unknown_fields__, unknowns))

            {:error, _} ->
              acc
          end
        end
      end
    ]

    # Generate encode field expressions
    encode_exprs =
      Enum.map(fields, fn field ->
        generate_field_encode_expr(field, schema)
      end)

    quote do
      def unquote(decode_fn)(binary) when is_binary(binary) do
        unquote(decode_loop_fn)(binary, unquote(Macro.escape(default_map)))
      end

      unquote(terminal_clause)
      unquote_splicing(field_decode_clauses)
      unquote_splicing(unknown_field_clauses)

      def unquote(encode_fn)(data) when is_map(data) do
        unknown_iodata =
          case Map.get(data, :__unknown_fields__) || Map.get(data, "__unknown_fields__") do
            nil -> []
            bin when is_binary(bin) -> bin
            list when is_list(list) -> list
          end

        [unquote_splicing(encode_exprs), unknown_iodata]
      end

      def unquote(encode_fn)(data) when is_list(data) do
        unquote(encode_fn)(SimpleGrpc.Protobuf.kw_to_map(data))
      end
    end
  end

  defp generate_field_decode_clauses(loop_fn, %Field{kind: :oneof, fields: subfields}, schema) do
    Enum.flat_map(subfields, fn subfield ->
      generate_field_decode_clauses(loop_fn, subfield, schema)
    end)
  end

  defp generate_field_decode_clauses(
         loop_fn,
         %Field{tag_bytes: tag_bytes, wire_type: wire_type} = field,
         _schema
       )
       when is_binary(tag_bytes) and is_integer(wire_type) do
    field_name = field.name
    is_single_byte_tag = byte_size(tag_bytes) == 1

    case {wire_type, field.type, field.prefix} do
      # Repeated Submessage
      {2, sub_type, :repeated} when is_binary(sub_type) and field.submessage_desc != nil ->
        if is_single_byte_tag do
          [
            quote do
              defp unquote(loop_fn)(
                     <<unquote(tag_bytes)::binary, 0::1, len::7, sub_bytes::binary-size(len),
                       rest::binary>>,
                     acc
                   ) do
                sub = decode(unquote(sub_type), sub_bytes)

                unquote(loop_fn)(rest, %{
                  acc
                  | unquote(field_name) => [sub | acc.unquote(field_name)]
                })
              end
            end,
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<sub_bytes::binary-size(^len), rest3::binary>> = rest2
                sub = decode(unquote(sub_type), sub_bytes)

                unquote(loop_fn)(rest3, %{
                  acc
                  | unquote(field_name) => [sub | acc.unquote(field_name)]
                })
              end
            end
          ]
        else
          [
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<sub_bytes::binary-size(^len), rest3::binary>> = rest2
                sub = decode(unquote(sub_type), sub_bytes)

                unquote(loop_fn)(rest3, %{
                  acc
                  | unquote(field_name) => [sub | acc.unquote(field_name)]
                })
              end
            end
          ]
        end

      # Non-repeated Submessage
      {2, sub_type, _} when is_binary(sub_type) and field.submessage_desc != nil ->
        if is_single_byte_tag do
          [
            quote do
              defp unquote(loop_fn)(
                     <<unquote(tag_bytes)::binary, 0::1, len::7, sub_bytes::binary-size(len),
                       rest::binary>>,
                     acc
                   ) do
                sub = decode(unquote(sub_type), sub_bytes)
                unquote(loop_fn)(rest, %{acc | unquote(field_name) => sub})
              end
            end,
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<sub_bytes::binary-size(^len), rest3::binary>> = rest2
                sub = decode(unquote(sub_type), sub_bytes)
                unquote(loop_fn)(rest3, %{acc | unquote(field_name) => sub})
              end
            end
          ]
        else
          [
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<sub_bytes::binary-size(^len), rest3::binary>> = rest2
                sub = decode(unquote(sub_type), sub_bytes)
                unquote(loop_fn)(rest3, %{acc | unquote(field_name) => sub})
              end
            end
          ]
        end

      # Repeated String
      {2, "string", :repeated} ->
        if is_single_byte_tag do
          [
            quote do
              defp unquote(loop_fn)(
                     <<unquote(tag_bytes)::binary, 0::1, len::7, str::binary-size(len),
                       rest::binary>>,
                     acc
                   ) do
                unquote(loop_fn)(rest, %{
                  acc
                  | unquote(field_name) => [str | acc.unquote(field_name)]
                })
              end
            end,
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<str::binary-size(^len), rest3::binary>> = rest2

                unquote(loop_fn)(rest3, %{
                  acc
                  | unquote(field_name) => [str | acc.unquote(field_name)]
                })
              end
            end
          ]
        else
          [
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<str::binary-size(^len), rest3::binary>> = rest2

                unquote(loop_fn)(rest3, %{
                  acc
                  | unquote(field_name) => [str | acc.unquote(field_name)]
                })
              end
            end
          ]
        end

      # Non-repeated String
      {2, "string", _} ->
        if is_single_byte_tag do
          [
            quote do
              defp unquote(loop_fn)(
                     <<unquote(tag_bytes)::binary, 0::1, len::7, str::binary-size(len),
                       rest::binary>>,
                     acc
                   ) do
                unquote(loop_fn)(rest, %{acc | unquote(field_name) => str})
              end
            end,
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<str::binary-size(^len), rest3::binary>> = rest2
                unquote(loop_fn)(rest3, %{acc | unquote(field_name) => str})
              end
            end
          ]
        else
          [
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<str::binary-size(^len), rest3::binary>> = rest2
                unquote(loop_fn)(rest3, %{acc | unquote(field_name) => str})
              end
            end
          ]
        end

      # Repeated Bytes
      {2, "bytes", :repeated} ->
        [
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
              {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
              <<b::binary-size(^len), rest3::binary>> = rest2

              unquote(loop_fn)(rest3, %{
                acc
                | unquote(field_name) => [b | acc.unquote(field_name)]
              })
            end
          end
        ]

      # Non-repeated Bytes
      {2, "bytes", _} ->
        [
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
              {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
              <<b::binary-size(^len), rest3::binary>> = rest2
              unquote(loop_fn)(rest3, %{acc | unquote(field_name) => b})
            end
          end
        ]

      # Enum (Wire Type 0)
      {0, _enum_type, prefix} when field.enum_desc != nil ->
        enum_desc_escaped = Macro.escape(field.enum_desc)

        if prefix == :repeated do
          packed_tag =
            SimpleGrpc.Protobuf.encode_tag(field.number, 2)
            |> IO.iodata_to_binary()

          [
            quote do
              defp unquote(loop_fn)(<<unquote(packed_tag)::binary, rest1::binary>>, acc) do
                {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                <<packed_bytes::binary-size(^len), rest3::binary>> = rest2

                items =
                  SimpleGrpc.Protobuf.decode_packed_values(
                    packed_bytes,
                    unquote(field.type),
                    nil,
                    nil
                  )

                unquote(loop_fn)(rest3, %{
                  acc
                  | unquote(field_name) => Enum.reverse(items) ++ acc.unquote(field_name)
                })
              end
            end,
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {val, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)

                atom =
                  SimpleGrpc.Protobuf.resolve_enum_integer_to_atom(
                    val,
                    unquote(enum_desc_escaped)
                  )

                unquote(loop_fn)(rest2, %{
                  acc
                  | unquote(field_name) => [atom | acc.unquote(field_name)]
                })
              end
            end
          ]
        else
          [
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {val, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)

                atom =
                  SimpleGrpc.Protobuf.resolve_enum_integer_to_atom(
                    val,
                    unquote(enum_desc_escaped)
                  )

                unquote(loop_fn)(rest2, %{acc | unquote(field_name) => atom})
              end
            end
          ]
        end

      # Double (Wire Type 1)
      {1, "double", :repeated} ->
        packed_tag =
          SimpleGrpc.Protobuf.encode_tag(field.number, 2)
          |> IO.iodata_to_binary()

        [
          quote do
            defp unquote(loop_fn)(<<unquote(packed_tag)::binary, rest1::binary>>, acc) do
              {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
              <<packed_bytes::binary-size(^len), rest3::binary>> = rest2

              items =
                SimpleGrpc.Protobuf.decode_packed_values(packed_bytes, "double", nil, nil)

              unquote(loop_fn)(rest3, %{
                acc
                | unquote(field_name) => Enum.reverse(items) ++ acc.unquote(field_name)
              })
            end
          end,
          quote do
            defp unquote(loop_fn)(
                   <<unquote(tag_bytes)::binary, val::float-little-size(64), rest::binary>>,
                   acc
                 ) do
              unquote(loop_fn)(rest, %{
                acc
                | unquote(field_name) => [val | acc.unquote(field_name)]
              })
            end
          end
        ]

      {1, "double", _} ->
        [
          quote do
            defp unquote(loop_fn)(
                   <<unquote(tag_bytes)::binary, val::float-little-size(64), rest::binary>>,
                   acc
                 ) do
              unquote(loop_fn)(rest, %{acc | unquote(field_name) => val})
            end
          end
        ]

      # Float (Wire Type 5)
      {5, "float", :repeated} ->
        packed_tag =
          SimpleGrpc.Protobuf.encode_tag(field.number, 2)
          |> IO.iodata_to_binary()

        [
          quote do
            defp unquote(loop_fn)(<<unquote(packed_tag)::binary, rest1::binary>>, acc) do
              {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
              <<packed_bytes::binary-size(^len), rest3::binary>> = rest2

              items =
                SimpleGrpc.Protobuf.decode_packed_values(packed_bytes, "float", nil, nil)

              unquote(loop_fn)(rest3, %{
                acc
                | unquote(field_name) => Enum.reverse(items) ++ acc.unquote(field_name)
              })
            end
          end,
          quote do
            defp unquote(loop_fn)(
                   <<unquote(tag_bytes)::binary, val::float-little-size(32), rest::binary>>,
                   acc
                 ) do
              unquote(loop_fn)(rest, %{
                acc
                | unquote(field_name) => [val | acc.unquote(field_name)]
              })
            end
          end
        ]

      {5, "float", _} ->
        [
          quote do
            defp unquote(loop_fn)(
                   <<unquote(tag_bytes)::binary, val::float-little-size(32), rest::binary>>,
                   acc
                 ) do
              unquote(loop_fn)(rest, %{acc | unquote(field_name) => val})
            end
          end
        ]

      # Bool (Wire Type 0)
      {0, "bool", :repeated} ->
        packed_tag =
          SimpleGrpc.Protobuf.encode_tag(field.number, 2)
          |> IO.iodata_to_binary()

        [
          quote do
            defp unquote(loop_fn)(<<unquote(packed_tag)::binary, rest1::binary>>, acc) do
              {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
              <<packed_bytes::binary-size(^len), rest3::binary>> = rest2

              items =
                SimpleGrpc.Protobuf.decode_packed_values(packed_bytes, "bool", nil, nil)

              unquote(loop_fn)(rest3, %{
                acc
                | unquote(field_name) => Enum.reverse(items) ++ acc.unquote(field_name)
              })
            end
          end,
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, 0, rest::binary>>, acc) do
              unquote(loop_fn)(rest, %{
                acc
                | unquote(field_name) => [false | acc.unquote(field_name)]
              })
            end
          end,
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, 1, rest::binary>>, acc) do
              unquote(loop_fn)(rest, %{
                acc
                | unquote(field_name) => [true | acc.unquote(field_name)]
              })
            end
          end
        ]

      {0, "bool", _} ->
        [
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, 0, rest::binary>>, acc) do
              unquote(loop_fn)(rest, %{acc | unquote(field_name) => false})
            end
          end,
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, 1, rest::binary>>, acc) do
              unquote(loop_fn)(rest, %{acc | unquote(field_name) => true})
            end
          end
        ]

      # Varint Integers
      {0, t, :repeated} when t in ["int32", "int64", "uint32", "uint64"] ->
        packed_tag =
          SimpleGrpc.Protobuf.encode_tag(field.number, 2)
          |> IO.iodata_to_binary()

        packed_clauses = [
          quote do
            defp unquote(loop_fn)(<<unquote(packed_tag)::binary, rest1::binary>>, acc) do
              {len, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
              <<packed_bytes::binary-size(^len), rest3::binary>> = rest2

              items =
                SimpleGrpc.Protobuf.decode_packed_values(packed_bytes, unquote(t), nil, nil)

              unquote(loop_fn)(rest3, %{
                acc
                | unquote(field_name) => Enum.reverse(items) ++ acc.unquote(field_name)
              })
            end
          end
        ]

        unpacked_clauses =
          if is_single_byte_tag do
            [
              quote do
                defp unquote(loop_fn)(
                       <<unquote(tag_bytes)::binary, 0::1, val::7, rest::binary>>,
                       acc
                     ) do
                  unquote(loop_fn)(rest, %{
                    acc
                    | unquote(field_name) => [val | acc.unquote(field_name)]
                  })
                end
              end,
              quote do
                defp unquote(loop_fn)(
                       <<unquote(tag_bytes)::binary, 1::1, v1::7, 0::1, v2::7, rest::binary>>,
                       acc
                     ) do
                  unquote(loop_fn)(rest, %{
                    acc
                    | unquote(field_name) => [v1 ||| v2 <<< 7 | acc.unquote(field_name)]
                  })
                end
              end,
              quote do
                defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                  {val, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)

                  unquote(loop_fn)(rest2, %{
                    acc
                    | unquote(field_name) => [val | acc.unquote(field_name)]
                  })
                end
              end
            ]
          else
            [
              quote do
                defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                  {val, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)

                  unquote(loop_fn)(rest2, %{
                    acc
                    | unquote(field_name) => [val | acc.unquote(field_name)]
                  })
                end
              end
            ]
          end

        packed_clauses ++ unpacked_clauses

      {0, t, _} when t in ["int32", "int64", "uint32", "uint64"] ->
        if is_single_byte_tag do
          [
            quote do
              defp unquote(loop_fn)(
                     <<unquote(tag_bytes)::binary, 0::1, val::7, rest::binary>>,
                     acc
                   ) do
                unquote(loop_fn)(rest, %{acc | unquote(field_name) => val})
              end
            end,
            quote do
              defp unquote(loop_fn)(
                     <<unquote(tag_bytes)::binary, 1::1, v1::7, 0::1, v2::7, rest::binary>>,
                     acc
                   ) do
                unquote(loop_fn)(rest, %{acc | unquote(field_name) => v1 ||| v2 <<< 7})
              end
            end,
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {val, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                unquote(loop_fn)(rest2, %{acc | unquote(field_name) => val})
              end
            end
          ]
        else
          [
            quote do
              defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
                {val, rest2} = SimpleGrpc.Protobuf.decode_varint(rest1)
                unquote(loop_fn)(rest2, %{acc | unquote(field_name) => val})
              end
            end
          ]
        end

      # Map field
      {2, {:map, _k_type, _v_type}, _} ->
        [
          quote do
            defp unquote(loop_fn)(<<unquote(tag_bytes)::binary, rest1::binary>>, acc) do
              {{k, v}, rest2} = SimpleGrpc.Protobuf.decode_map_entry(rest1, unquote(Macro.escape(field)))
              curr_map = Map.get(acc, unquote(field_name), %{})
              unquote(loop_fn)(rest2, %{acc | unquote(field_name) => Map.put(curr_map, k, v)})
            end
          end
        ]

      _other ->
        []
    end
  end

  defp generate_field_decode_clauses(_loop_fn, _field, _schema), do: []

  defp generate_field_encode_expr(%Field{kind: :oneof, fields: subfields}, schema) do
    branches =
      Enum.map(subfields, fn subfield ->
        name_atom = subfield.name
        name_str = Atom.to_string(name_atom)
        sub_expr = generate_single_field_encode_expr(subfield, quote(do: v), schema, true)

        quote do
          case data do
            %{unquote(name_atom) => v} when not is_nil(v) -> unquote(sub_expr)
            %{unquote(name_str) => v} when not is_nil(v) -> unquote(sub_expr)
            _ -> []
          end
        end
      end)

    branches
  end

  defp generate_field_encode_expr(%Field{prefix: :repeated} = field, _schema) do
    name_atom = field.name
    name_str = Atom.to_string(name_atom)
    tag_bytes = field.tag_bytes

    if field.submessage_desc != nil do
      sub_type = field.type

      quote do
        case data do
          %{unquote(name_atom) => list} when is_list(list) and list != [] ->
            Enum.map(list, fn item ->
              sub_bin = encode(unquote(sub_type), item)

              [
                unquote(tag_bytes),
                SimpleGrpc.Protobuf.encode_varint(IO.iodata_length(sub_bin)),
                sub_bin
              ]
            end)

          %{unquote(name_str) => list} when is_list(list) and list != [] ->
            Enum.map(list, fn item ->
              sub_bin = encode(unquote(sub_type), item)

              [
                unquote(tag_bytes),
                SimpleGrpc.Protobuf.encode_varint(IO.iodata_length(sub_bin)),
                sub_bin
              ]
            end)

          _ ->
            []
        end
      end
    else
      if field.type == "string" do
        quote do
          case data do
            %{unquote(name_atom) => list} when is_list(list) and list != [] ->
              Enum.map(list, fn s ->
                [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(byte_size(s)), s]
              end)

            %{unquote(name_str) => list} when is_list(list) and list != [] ->
              Enum.map(list, fn s ->
                [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(byte_size(s)), s]
              end)

            _ ->
              []
          end
        end
      else
        quote do
          case data do
            %{unquote(name_atom) => list} when is_list(list) and list != [] ->
              SimpleGrpc.Protobuf.encode_repeated(unquote(Macro.escape(field)), list, nil, nil)

            %{unquote(name_str) => list} when is_list(list) and list != [] ->
              SimpleGrpc.Protobuf.encode_repeated(unquote(Macro.escape(field)), list, nil, nil)

            _ ->
              []
          end
        end
      end
    end
  end

  defp generate_field_encode_expr(%Field{prefix: :required} = field, schema) do
    name_atom = field.name
    name_str = Atom.to_string(name_atom)

    encode_expr =
      generate_single_field_encode_expr(field, quote(do: v), schema, true)

    quote do
      case data do
        %{unquote(name_atom) => v} when not is_nil(v) -> unquote(encode_expr)
        %{unquote(name_str) => v} when not is_nil(v) -> unquote(encode_expr)
        _ -> throw({:error, unquote("missing required field '#{name_atom}'")})
      end
    end
  end

  defp generate_field_encode_expr(%Field{kind: :map} = field, _schema) do
    name_atom = field.name
    name_str = Atom.to_string(name_atom)

    quote do
      case data do
        %{unquote(name_atom) => map} when is_map(map) and map_size(map) > 0 ->
          SimpleGrpc.Protobuf.encode_map_entries(unquote(Macro.escape(field)), map, nil, nil)

        %{unquote(name_str) => map} when is_map(map) and map_size(map) > 0 ->
          SimpleGrpc.Protobuf.encode_map_entries(unquote(Macro.escape(field)), map, nil, nil)

        _ ->
          []
      end
    end
  end

  defp generate_field_encode_expr(%Field{} = field, schema) do
    name_atom = field.name
    name_str = Atom.to_string(name_atom)

    encode_expr =
      generate_single_field_encode_expr(field, quote(do: v), schema, field.prefix == :optional)

    quote do
      case data do
        %{unquote(name_atom) => v} when not is_nil(v) -> unquote(encode_expr)
        %{unquote(name_str) => v} when not is_nil(v) -> unquote(encode_expr)
        _ -> []
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{type: "string", tag_bytes: tag_bytes},
         val_ast,
         _schema,
         is_oneof
       ) do
    if is_oneof do
      quote do
        case unquote(val_ast) do
          v when is_binary(v) ->
            [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(byte_size(v)), v]

          v when is_atom(v) and not is_nil(v) ->
            s = Atom.to_string(v)
            [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(byte_size(s)), s]

          _ ->
            []
        end
      end
    else
      quote do
        case unquote(val_ast) do
          v when is_binary(v) and v != "" ->
            [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(byte_size(v)), v]

          v when is_atom(v) and not is_nil(v) ->
            s = Atom.to_string(v)
            [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(byte_size(s)), s]

          _ ->
            []
        end
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{type: "double", tag_bytes: tag_bytes},
         val_ast,
         _schema,
         is_oneof
       ) do
    if is_oneof do
      quote do
        case unquote(val_ast) do
          v when is_number(v) ->
            [unquote(tag_bytes), <<v * 1.0::float-little-size(64)>>]

          _ ->
            []
        end
      end
    else
      quote do
        case unquote(val_ast) do
          v when is_number(v) and v != 0 and v != 0.0 ->
            [unquote(tag_bytes), <<v * 1.0::float-little-size(64)>>]

          _ ->
            []
        end
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{type: "float", tag_bytes: tag_bytes},
         val_ast,
         _schema,
         is_oneof
       ) do
    if is_oneof do
      quote do
        case unquote(val_ast) do
          v when is_number(v) ->
            [unquote(tag_bytes), <<v * 1.0::float-little-size(32)>>]

          _ ->
            []
        end
      end
    else
      quote do
        case unquote(val_ast) do
          v when is_number(v) and v != 0 and v != 0.0 ->
            [unquote(tag_bytes), <<v * 1.0::float-little-size(32)>>]

          _ ->
            []
        end
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{type: "bool", tag_bytes: tag_bytes},
         val_ast,
         _schema,
         is_oneof
       ) do
    if is_oneof do
      quote do
        case unquote(val_ast) do
          false -> [unquote(tag_bytes), <<0>>]
          _ -> [unquote(tag_bytes), <<1>>]
        end
      end
    else
      quote do
        case unquote(val_ast) do
          true -> [unquote(tag_bytes), <<1>>]
          _ -> []
        end
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{submessage_desc: %Message{}} = field,
         val_ast,
         _schema,
         _is_oneof
       ) do
    sub_type = field.type
    tag_bytes = field.tag_bytes

    quote do
      case unquote(val_ast) do
        v when is_map(v) and map_size(v) > 0 ->
          sub_bin = encode(unquote(sub_type), v)

          [
            unquote(tag_bytes),
            SimpleGrpc.Protobuf.encode_varint(IO.iodata_length(sub_bin)),
            sub_bin
          ]

        _ ->
          []
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{enum_desc: %SchemaEnum{} = enum_desc} = field,
         val_ast,
         _schema,
         is_oneof
       ) do
    tag_bytes = field.tag_bytes
    enum_desc_escaped = Macro.escape(enum_desc)

    if is_oneof do
      quote do
        int_val =
          SimpleGrpc.Protobuf.resolve_enum_value_to_integer(
            unquote(val_ast),
            unquote(enum_desc_escaped)
          )

        [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(int_val)]
      end
    else
      quote do
        case unquote(val_ast) do
          v ->
            int_val =
              SimpleGrpc.Protobuf.resolve_enum_value_to_integer(v, unquote(enum_desc_escaped))

            if int_val != 0 do
              [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(int_val)]
            else
              []
            end
        end
      end
    end
  end

  defp generate_single_field_encode_expr(
         %Field{tag_bytes: tag_bytes, type: t},
         val_ast,
         _schema,
         is_oneof
       )
       when t in ["int32", "int64", "uint32", "uint64"] do
    if is_oneof do
      quote do
        case unquote(val_ast) do
          v when is_integer(v) ->
            [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(v)]

          _ ->
            []
        end
      end
    else
      quote do
        case unquote(val_ast) do
          v when is_integer(v) and v != 0 ->
            [unquote(tag_bytes), SimpleGrpc.Protobuf.encode_varint(v)]

          _ ->
            []
        end
      end
    end
  end

  defp generate_single_field_encode_expr(field, val_ast, _schema, _is_oneof) do
    quote do
      SimpleGrpc.Protobuf.encode_single_field(
        unquote(field.number),
        unquote(Macro.escape(field.type)),
        unquote(val_ast),
        nil,
        nil,
        unquote(field.tag_bytes),
        unquote(field.wire_type)
      )
    end
  end

  defp msg_fun_name(prefix, full_name) do
    sanitized =
      full_name
      |> to_string()
      |> String.replace(".", "_")
      |> String.replace("/", "_")

    :"#{prefix}_#{sanitized}"
  end
end
