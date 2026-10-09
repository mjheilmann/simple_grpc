defmodule SimpleGrpc.Parser.Proto2 do
  # proto2 spec https://protobuf.dev/reference/protobuf/proto2-spec/

  import NimbleParsec

  alias SimpleGrpc.Parser.Proto2.Core
  alias SimpleGrpc.Parser.Proto2.Message
  alias SimpleGrpc.Parser.Proto2.Service
  alias SimpleGrpc.Parser.Proto2.Enum, as: ProtoEnum

  # Syntax Statement: syntax = "proto2" ;
  syntax_stmt =
    ignore(string("syntax"))
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> ignore(choice([string("\""), string("'")]))
    |> string("proto2")
    |> unwrap_and_tag(:syntax)
    |> ignore(choice([string("\""), string("'")]))
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))

  # package foo.bar;
  package_stmt =
    ignore(string("package"))
    |> concat(Core.space_or_comment())
    |> concat(Core.full_ident())
    |> unwrap_and_tag(:package)
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))

  # String Literals
  str_lit =
    choice([
      ignore(string("\"")) |> concat(ascii_string([not: ?\"], min: 0)) |> ignore(string("\"")),
      ignore(string("'")) |> concat(ascii_string([not: ?'], min: 0)) |> ignore(string("'"))
    ])
    |> reduce({Enum, :join, []})

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
      syntax_stmt,
      package_stmt,
      Core.option_statement(),
      import_stmt,
      extend_stmt,
      parsec(:recurse_message),
      parsec(:recurse_service),
      parsec(:recurse_enum),
      unhandled_chunk
    ])

  # Root Protocol Buffer Document Definition
  proto_doc =
    repeat(statement)
    |> concat(Core.space_or_comment())
    |> eos()

  defparsec(:recurse_message, Message.combinator())
  defparsec(:recurse_service, Service.combinator())
  defparsec(:recurse_enum, ProtoEnum.combinator())
  defparsec(:parse, proto_doc)
end
