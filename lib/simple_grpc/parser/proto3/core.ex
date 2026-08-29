defmodule SimpleGrpc.Parser.Proto3.Core do
  import NimbleParsec

  def space_or_comment do
    whitespace = ascii_string([?\s, ?\t, ?\r, ?\n], min: 1)

    line_comment =
      string("//")
      |> concat(ascii_string([not: ?\n], min: 0))
      |> concat(choice([string("\n"), eos()]))

    block_comment =
      string("/*")
      |> concat(
        repeat(
          choice([ascii_string([not: ?*], min: 1), string("*") |> lookahead_not(string("/"))])
        )
      )
      |> string("*/")

    choice([whitespace, line_comment, block_comment]) |> repeat() |> ignore()
  end

  def ident do
    ascii_string([?a..?z, ?A..?Z, ?_], 1)
    |> concat(ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 0))
    |> reduce({Enum, :join, []})
  end

  def full_ident do
    ident() |> repeat(ignore(string(".")) |> concat(ident()))
  end
end
