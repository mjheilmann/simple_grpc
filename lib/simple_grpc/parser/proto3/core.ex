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
    ident()
    |> repeat(string(".") |> concat(ident()))
    |> reduce({Enum, :join, []})
  end

  def option_name do
    custom_option_part =
      string("(")
      |> concat(space_or_comment())
      |> concat(full_ident())
      |> concat(space_or_comment())
      |> concat(string(")"))

    choice([
      custom_option_part,
      ident()
    ])
    |> repeat(
      string(".")
      |> concat(ident())
    )
    |> reduce({Enum, :join, []})
  end

  def option_statement do
    ignore(string("option"))
    |> concat(space_or_comment())
    |> concat(option_name())
    |> concat(space_or_comment())
    |> ignore(string("="))
    |> concat(space_or_comment())
    |> concat(option_value())
    |> concat(space_or_comment())
    |> ignore(string(";"))
    |> tag(:option)
  end

  def option_value do
    str_lit =
      choice([
        ignore(string("\"")) |> utf8_string([not: ?"], min: 0) |> ignore(string("\"")),
        ignore(string("'")) |> utf8_string([not: ?'], min: 0) |> ignore(string("'"))
      ])

    choice([
      str_lit,
      string("true") |> replace(true),
      string("false") |> replace(false),
      integer(min: 1),
      full_ident()
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
