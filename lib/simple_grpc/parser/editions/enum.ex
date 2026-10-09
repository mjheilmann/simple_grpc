defmodule SimpleGrpc.Parser.Editions.Enum do
  import NimbleParsec
  alias SimpleGrpc.Parser.Editions.Core

  def calculate_sign([sign_modifier, numeric_value]) do
    sign_modifier * numeric_value
  end

  sign =
    choice([
      string("-") |> replace(-1),
      empty() |> replace(1)
    ])

  digits =
    choice([
      integer(min: 1),
      string("0") |> replace(0)
    ])

  enum_value =
    sign
    |> concat(digits)
    |> reduce({__MODULE__, :calculate_sign, []})

  enum_field =
    Core.ident()
    |> concat(Core.space_or_comment())
    |> ignore(string("="))
    |> concat(Core.space_or_comment())
    |> concat(enum_value)
    |> concat(Core.space_or_comment())
    |> optional(Core.field_options())
    |> concat(Core.space_or_comment())
    |> ignore(string(";"))
    |> tag(:enum_field)

  enum_combinator =
    ignore(string("enum"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      choice([
        Core.option_statement(),
        enum_field
      ])
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:enum)

  def combinator do
    unquote(Macro.escape(enum_combinator))
  end
end
