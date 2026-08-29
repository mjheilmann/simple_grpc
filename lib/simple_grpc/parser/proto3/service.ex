defmodule SimpleGrpc.Parser.Proto3.Service do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto3.Core

  stream_modifier =
    optional(
      string("stream")
      |> concat(Core.space_or_comment())
      |> replace(:stream)
    )

  # 1. Individual RPC Method parser
  rpc_method =
    ignore(string("rpc"))
    |> concat(Core.space_or_comment())
    # Method Name
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("("))
    |> concat(Core.space_or_comment())

    # NEW: Match streaming input
    |> concat(stream_modifier)
    # Input Type
    |> concat(Core.full_ident())
    |> concat(Core.space_or_comment())
    |> ignore(string(")"))
    |> concat(Core.space_or_comment())
    |> ignore(string("returns"))
    |> concat(Core.space_or_comment())
    |> ignore(string("("))
    |> concat(Core.space_or_comment())

    # NEW: Match streaming output
    |> concat(stream_modifier)
    # Output Type
    |> concat(Core.full_ident())
    |> concat(Core.space_or_comment())
    |> ignore(string(")"))
    |> concat(Core.space_or_comment())
    |> choice([
      ignore(string(";")),
      ignore(string("{")) |> concat(Core.space_or_comment()) |> ignore(string("}"))
    ])
    |> tag(:rpc)

  # 2. Main Service block parser
  service_combinator =
    ignore(string("service"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      rpc_method
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:service)

  # FIXED: Wrapped in Macro.escape/1 to cleanly pass the AST back to the central compiler file
  def combinator do
    unquote(Macro.escape(service_combinator))
  end
end
