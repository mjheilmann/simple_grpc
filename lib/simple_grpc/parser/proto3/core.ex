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

  def option_statement do
    ignore(string("option"))
    |> concat(space_or_comment())
    # Captures the option name (e.g., "go_package" or "deprecated")
    |> concat(ident())
    |> concat(space_or_comment())
    |> ignore(string("="))
    |> concat(space_or_comment())
    # Captures the string or boolean
    |> concat(option_value())
    |> concat(space_or_comment())
    |> ignore(string(";"))
    |> tag(:option)
  end

  def option_value do
    quoted_string =
      ignore(string("\""))
      # Match 0 or more characters, as long as it isn't a double-quote character code (34)
      |> utf8_string([not: ?"], min: 0)
      |> ignore(string("\""))

    choice([
      quoted_string,
      string("true") |> replace(true),
      string("false") |> replace(false)
    ])
  end

  def field_options do
    ignore(string("["))
    |> concat(space_or_comment())
    |> ignore(string("deprecated"))
    |> concat(space_or_comment())
    |> ignore(string("="))
    |> concat(space_or_comment())
    |> choice([
      string("true") |> replace(true),
      string("false") |> replace(false)
    ])
    |> concat(space_or_comment())
    |> ignore(string("]"))
    |> tag(:deprecated)
  end
end
