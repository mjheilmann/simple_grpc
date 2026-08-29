defmodule SimpleGrpc.Parser.Proto3.RPC do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto3.Core

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
    # Method Name
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> concat(argument())
    # |> tag(:input)
    |> concat(Core.space_or_comment())
    |> ignore(string("returns"))
    |> concat(Core.space_or_comment())
    |> concat(argument())
    # |> tag(:output)
    |> concat(Core.space_or_comment())

    # Choice for closing brace or semicolon
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
