defmodule SimpleGrpc.Parser.Proto2.Message do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto2.Core

  @types ~w(double float int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64 bool string bytes)a

  primitive_types = Enum.map(@types, &string(Atom.to_string(&1)))
  field_type = choice(primitive_types ++ [Core.full_ident()])

  # Field prefix modifiers: required, optional, repeated
  field_prefix =
    choice([
      string("required") |> replace(:required),
      string("optional") |> replace(:optional),
      string("repeated") |> replace(:repeated)
    ])
    |> unwrap_and_tag(:prefix)

  # Standard field
  normal_field =
    optional(field_prefix |> concat(Core.space_or_comment()))
    |> concat(field_type)
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> integer(min: 1)
    |> concat(Core.space_or_comment())
    |> optional(Core.field_options())
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:field)

  # Oneof block
  oneof_block =
    ignore(string("oneof"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      normal_field
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:oneof)

  # Map field
  map_field =
    ignore(string("map"))
    |> concat(Core.space_or_comment())
    |> ignore(string("<"))
    |> concat(Core.space_or_comment())
    |> concat(field_type)
    |> concat(Core.space_or_comment())
    |> ignore(string(","))
    |> concat(Core.space_or_comment())
    |> concat(field_type)
    |> concat(Core.space_or_comment())
    |> ignore(string(">"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> integer(min: 1)
    |> concat(Core.space_or_comment())
    |> optional(Core.field_options())
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:map)

  # Reserved names
  reserved_name =
    choice([
      ignore(string("\"")) |> concat(ascii_string([not: ?\"], min: 1)) |> ignore(string("\"")),
      ignore(string("'")) |> concat(ascii_string([not: ?'], min: 1)) |> ignore(string("'"))
    ])

  # Reserved ranges
  reserved_range =
    integer(min: 1)
    |> optional(
      concat(Core.space_or_comment(), ignore(string("to")))
      |> concat(Core.space_or_comment())
      |> concat(choice([integer(min: 1), string("max")]))
    )
    |> tag(:range)

  reserved_stmt =
    ignore(string("reserved"))
    |> concat(Core.space_or_comment())
    |> choice([
      reserved_name
      |> repeat(ignore(string(",")) |> concat(Core.space_or_comment()) |> concat(reserved_name)),
      reserved_range
      |> repeat(ignore(string(",")) |> concat(Core.space_or_comment()) |> concat(reserved_range))
    ])
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:reserved)

  # Extensions range parser
  def parse_ext_range([from, to]) do
    to_val = if to == "max", do: :max, else: to
    {from, to_val}
  end

  def parse_ext_range([single]) do
    {single, single}
  end

  extension_range_item =
    integer(min: 1)
    |> optional(
      concat(Core.space_or_comment(), ignore(string("to")))
      |> concat(Core.space_or_comment())
      |> concat(choice([integer(min: 1), string("max")]))
    )
    |> reduce({__MODULE__, :parse_ext_range, []})

  extensions_stmt =
    ignore(string("extensions"))
    |> concat(Core.space_or_comment())
    |> concat(extension_range_item)
    |> repeat(
      concat(Core.space_or_comment(), ignore(string(",")))
      |> concat(Core.space_or_comment())
      |> concat(extension_range_item)
    )
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:extensions)

  # Extend block
  extend_block =
    ignore(string("extend"))
    |> concat(Core.space_or_comment())
    |> concat(Core.full_ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      normal_field
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:extend)

  # Group block
  group_block =
    optional(field_prefix |> concat(Core.space_or_comment()))
    |> ignore(string("group"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> integer(min: 1)
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      choice([
        normal_field,
        parsec(:recurse_message),
        parsec(:recurse_enum)
      ])
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:group)

  message_combinator =
    ignore(string("message"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      choice([
        Core.option_statement(),
        normal_field,
        oneof_block,
        map_field,
        reserved_stmt,
        extensions_stmt,
        extend_block,
        group_block,
        parsec(:recurse_message),
        parsec(:recurse_enum)
      ])
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:message)

  def combinator do
    unquote(Macro.escape(message_combinator))
  end

  def normal_field_combinator do
    unquote(Macro.escape(normal_field))
  end

  def extend_block_combinator do
    unquote(Macro.escape(extend_block))
  end
end
