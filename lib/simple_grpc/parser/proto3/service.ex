defmodule SimpleGrpc.Parser.Proto3.Service do
  import NimbleParsec
  alias SimpleGrpc.Parser.Proto3.Core
  alias SimpleGrpc.Parser.Proto3.RPC

  # 2. Main Service block parser with service-level options
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

  # FIXED: Wrapped in Macro.escape/1 to cleanly pass the AST back to the central compiler file
  def combinator do
    unquote(Macro.escape(service_combinator))
  end
end
