defmodule SimpleGrpc.Parser.Proto2.Service do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto2.Core
  alias SimpleGrpc.Parser.Proto2.RPC

  service_combinator =
    ignore(string("service"))
    |> concat(Core.space_or_comment())
    |> concat(Core.ident())
    |> concat(Core.space_or_comment())
    |> ignore(string("{"))
    |> concat(Core.space_or_comment())
    |> repeat(
      choice([
        Core.option_statement(),
        RPC.rpc_statement()
      ])
      |> concat(Core.space_or_comment())
    )
    |> ignore(string("}"))
    |> tag(:service)

  def combinator do
    unquote(Macro.escape(service_combinator))
  end
end
