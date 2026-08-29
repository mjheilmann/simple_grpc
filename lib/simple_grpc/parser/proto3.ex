defmodule SimpleGrpc.Parser.Proto3 do
  # proto3 spec https://protobuf.dev/reference/protobuf/proto3-spec/

  import NimbleParsec

  alias SimpleGrpc.Parser.Proto3.Core
  alias SimpleGrpc.Parser.Proto3.Message
  alias SimpleGrpc.Parser.Proto3.Service
  alias SimpleGrpc.Parser.Proto3.Enum, as: ProtoEnum

  # Syntax Statement: syntax = "proto3" ;
  syntax_stmt =
    ignore(string("syntax"))
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> ignore(choice([string("\""), string("'")]))
    |> string("proto3")
    |> unwrap_and_tag(:syntax)
    |> ignore(choice([string("\""), string("'")]))
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))

  # package edge_cases;
  package_stmt =
    ignore(string("package"))
    |> concat(Core.space_or_comment())
    |> concat(Core.full_ident())
    # Changed to tag/2 so nested arrays like ["foo", "bar"] don't get unwrapped/dropped
    |> unwrap_and_tag(:package)
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))

  # 1. String Literals (e.g., "other.proto" or 'other.proto')
  # Matches content inside quotes and combines it into a single string
  str_lit =
    choice([
      ignore(string("\"")) |> concat(ascii_string([not: ?\"], min: 0)) |> ignore(string("\"")),
      ignore(string("'")) |> concat(ascii_string([not: ?'], min: 0)) |> ignore(string("'"))
    ])
    |> reduce({Enum, :join, []})

  # 2. Modifier keywords: [ "weak" | "public" ]
  import_modifier =
    choice([
      string("weak"),
      string("public")
    ])
    |> unwrap_and_tag(:modifier)

  # 3. Full Import Statement
  import_stmt =
    ignore(string("import"))
    |> concat(Core.space_or_comment())
    |> optional(import_modifier |> concat(Core.space_or_comment()))
    |> concat(str_lit)
    |> unwrap_and_tag(:path)
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:import)

  # Unhandled chunk: captures any remaining content, including field options like 'deprecated = true;'
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
      parsec(:recurse_message),
      parsec(:recurse_service),
      parsec(:recurse_enum),
      unhandled_chunk
    ])

  # Root Protocol Buffer Document Definition
  proto_doc =
    repeat(statement)
    |> concat(Core.space_or_comment())
    # ingore rest of document until we can match its structures
    # |> concat(ignore(utf8_string([0..0x10FFFF], min: 0)))
    |> eos()

  defparsec(:recurse_message, Message.combinator())
  defparsec(:recurse_service, Service.combinator())
  defparsec(:recurse_enum, ProtoEnum.combinator())
  defparsec(:parse, proto_doc)
end
