defmodule SimpleGrpc.Parser.Proto3.Message do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto3.Core

  @types ~w(double float int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64 sfixed32 sfixed64 bool string bytes)a

  # Base components
  primitive_types = Enum.map(@types, &string(Atom.to_string(&1)))
  field_type = choice(primitive_types ++ [Core.full_ident()])

  # Helper to match: [deprecated = true]
  field_options =
    ignore(string("["))
    |> concat(Core.space_or_comment())
    |> ignore(string("deprecated"))
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> choice([
      string("true") |> replace(true),
      string("false") |> replace(false)
    ])
    |> concat(Core.space_or_comment())
    |> ignore(string("]"))
    # Emits {:deprecated, [true]} if present
    |> tag(:deprecated)

  # Standard fields: string name = 1;
  normal_field =
    field_type
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> integer(min: 1)
    |> concat(Core.space_or_comment())
    |> optional(field_options)
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:field)

  # Oneof block layout parser
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

  # Map fields parser
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
    |> ignore(string(";"))
    |> tag(:map)

  # NEW: Reserved Names (e.g. "foo", "bar")
  reserved_name =
    choice([
      ignore(string("\"")) |> concat(ascii_string([not: ?\"], min: 1)) |> ignore(string("\"")),
      ignore(string("'")) |> concat(ascii_string([not: ?'], min: 1)) |> ignore(string("'"))
    ])

  # NEW: Reserved Ranges (e.g. 2, 9 to 11, 100 to max)
  reserved_range =
    integer(min: 1)
    |> optional(
      concat(Core.space_or_comment(), ignore(string("to")))
      |> concat(Core.space_or_comment())
      |> concat(choice([integer(min: 1), string("max")]))
    )
    |> tag(:range)

  # NEW: Core Reserved Statement Parser
  # Consumes either a comma-separated list of ranges OR a comma-separated list of field names
  reserved_stmt =
    ignore(string("reserved"))
    |> concat(Core.space_or_comment())
    |> choice([
      # List of names
      reserved_name
      |> repeat(ignore(string(",")) |> concat(Core.space_or_comment()) |> concat(reserved_name)),
      # List of ranges
      reserved_range
      |> repeat(ignore(string(",")) |> concat(Core.space_or_comment()) |> concat(reserved_range))
    ])
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:reserved)

  # Main structural pipeline reading fields, oneofs, maps, reserved blocks, or nested messages
  message_combinator =
    ignore(string("message"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      choice([
        normal_field,
        oneof_block,
        map_field,
        # <-- Injected right here
        reserved_stmt,
        parsec(:recurse_message)
      ])
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:message)

  def combinator do
    unquote(Macro.escape(message_combinator))
  end
end
