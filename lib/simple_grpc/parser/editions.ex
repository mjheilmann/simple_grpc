defmodule SimpleGrpc.Parser.Editions do
  # Protobuf Editions spec https://protobuf.dev/editions/overview/

  import NimbleParsec

  alias SimpleGrpc.Parser.Editions.Core
  alias SimpleGrpc.Parser.Editions.Message
  alias SimpleGrpc.Parser.Editions.Service
  alias SimpleGrpc.Parser.Editions.Enum, as: ProtoEnum

  # String Literals
  str_lit =
    choice([
      ignore(string("\"")) |> concat(ascii_string([not: ?\"], min: 0)) |> ignore(string("\"")),
      ignore(string("'")) |> concat(ascii_string([not: ?'], min: 0)) |> ignore(string("'"))
    ])
    |> reduce({Enum, :join, []})

  # Edition Statement: edition = "2023"; or syntax = "editions";
  edition_stmt =
    choice([
      ignore(string("edition"))
      |> concat(Core.space_or_comment())
      |> ignore(string("="))
      |> concat(Core.space_or_comment())
      |> concat(str_lit)
      |> unwrap_and_tag(:edition)
      |> concat(Core.space_or_comment())
      |> ignore(string(";")),
      ignore(string("syntax"))
      |> concat(Core.space_or_comment())
      |> ignore(string("="))
      |> concat(Core.space_or_comment())
      |> ignore(choice([string("\""), string("'")]))
      |> string("editions")
      |> replace("2023")
      |> unwrap_and_tag(:edition)
      |> ignore(choice([string("\""), string("'")]))
      |> concat(Core.space_or_comment())
      |> ignore(string(";"))
    ])

  # package foo.bar;
  package_stmt =
    ignore(string("package"))
    |> concat(Core.space_or_comment())
    |> concat(Core.full_ident())
    |> unwrap_and_tag(:package)
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))

  # Modifier keywords: [ "weak" | "public" ]
  import_modifier =
    choice([
      string("weak"),
      string("public")
    ])
    |> unwrap_and_tag(:modifier)

  # Full Import Statement
  import_stmt =
    ignore(string("import"))
    |> concat(Core.space_or_comment())
    |> optional(import_modifier |> concat(Core.space_or_comment()))
    |> concat(str_lit)
    |> unwrap_and_tag(:path)
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:import)

  # Top-level extend block
  extend_stmt = Message.extend_block_combinator()

  # Unhandled chunk
  unhandled_chunk =
    utf8_string([not: ?\s, not: ?\t, not: ?\r, not: ?\n], min: 1)
    |> unwrap_and_tag(:unhandled)

  statement =
    Core.space_or_comment()
    |> choice([
      edition_stmt,
      package_stmt,
      Core.option_statement(),
      import_stmt,
      extend_stmt,
      parsec(:recurse_message),
      parsec(:recurse_service),
      parsec(:recurse_enum),
      unhandled_chunk
    ])

  proto_doc =
    repeat(statement)
    |> concat(Core.space_or_comment())
    |> eos()

  defparsec(:recurse_message, Message.combinator())
  defparsec(:recurse_service, Service.combinator())
  defparsec(:recurse_enum, ProtoEnum.combinator())
  defparsec(:parse, proto_doc)
end
