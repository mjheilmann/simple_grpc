defmodule SimpleGrpc.Parser.Proto2.RPC do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto2.Core

  def stream_modifier do
    optional(
      string("stream")
      |> concat(Core.space_or_comment())
      |> replace(:stream)
    )
  end

  def argument do
    ignore(string("("))
    |> concat(Core.space_or_comment())
    |> concat(stream_modifier())
    |> concat(Core.full_ident())
    |> concat(Core.space_or_comment())
    |> ignore(string(")"))
  end

  def rpc_statement do
    ignore(string("rpc"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> concat(argument())
    |> concat(Core.space_or_comment())
    |> ignore(string("returns"))
    |> concat(Core.space_or_comment())
    |> concat(argument())
    |> concat(Core.space_or_comment())
    |> choice([
      ignore(string(";")),
      ignore(string("{"))
      |> concat(Core.space_or_comment())
      |> repeat(
        Core.option_statement()
        |> concat(Core.space_or_comment())
      )
      |> ignore(string("}"))
    ])
    |> tag(:rpc)
  end
end
