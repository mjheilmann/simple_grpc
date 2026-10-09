defmodule SimpleGrpc.Parser.Editions.Core do
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

  def custom_option_part do
    string("(")
    |> concat(space_or_comment())
    |> concat(full_ident())
    |> concat(space_or_comment())
    |> concat(string(")"))
    |> reduce({Enum, :join, []})
  end

  def option_name do
    choice([
      custom_option_part(),
      ident()
    ])
    |> repeat(
      string(".")
      |> concat(choice([custom_option_part(), ident()]))
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

  def parse_signed_int([sign_str, digits_int]) do
    if sign_str == "-", do: -digits_int, else: digits_int
  end

  def parse_signed_int([digits_int]), do: digits_int

  def parse_float(parts) do
    str = Enum.join(parts)

    case Float.parse(str) do
      {f, _} -> f
      :error -> str
    end
  end

  def option_value do
    str_lit =
      choice([
        ignore(string("\"")) |> utf8_string([not: ?"], min: 0) |> ignore(string("\"")),
        ignore(string("'")) |> utf8_string([not: ?'], min: 0) |> ignore(string("'"))
      ])

    float_lit =
      choice([string("-"), string("+"), empty()])
      |> concat(ascii_string([?0..?9], min: 1))
      |> concat(string("."))
      |> concat(ascii_string([?0..?9], min: 1))
      |> optional(
        choice([string("e"), string("E")])
        |> concat(choice([string("-"), string("+"), empty()]))
        |> concat(ascii_string([?0..?9], min: 1))
      )
      |> reduce({__MODULE__, :parse_float, []})

    special_float =
      choice([
        string("inf") |> replace(:infinity),
        string("infinity") |> replace(:infinity),
        string("-inf") |> replace(:"-infinity"),
        string("-infinity") |> replace(:"-infinity"),
        string("nan") |> replace(:nan)
      ])

    int_lit =
      choice([string("-"), string("+"), empty()])
      |> concat(integer(min: 1))
      |> reduce({__MODULE__, :parse_signed_int, []})

    zero_lit =
      choice([
        string("0") |> replace(0),
        string("-0") |> replace(0),
        string("+0") |> replace(0)
      ])

    bool_lit =
      choice([
        string("true") |> replace(true),
        string("false") |> replace(false)
      ])

    # In Protobuf options, unquoted identifiers or enum values (e.g. EXPLICIT, IMPLICIT, CLOSED, OPEN, PACKED, EXPANDED)
    ident_lit = full_ident()

    choice([
      str_lit,
      float_lit,
      special_float,
      int_lit,
      zero_lit,
      bool_lit,
      ident_lit
    ])
  end

  def field_option do
    option_name()
    |> concat(space_or_comment())
    |> ignore(string("="))
    |> concat(space_or_comment())
    |> concat(option_value())
    |> tag(:field_option)
  end

  def field_options do
    ignore(string("["))
    |> concat(space_or_comment())
    |> concat(field_option())
    |> repeat(
      concat(space_or_comment(), ignore(string(",")))
      |> concat(space_or_comment())
      |> concat(field_option())
    )
    |> concat(space_or_comment())
    |> ignore(string("]"))
    |> tag(:field_options)
  end
end
