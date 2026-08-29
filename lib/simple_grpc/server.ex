defmodule SimpleGrpc.Server do
  defmacro __using__(opts) do
    server_file = Keyword.fetch!(opts, :server_file)

    {:proto3, _service_name, ast} = SimpleGrpc.Parser.parse!(server_file)

    fields = [:syntax, :package, :import, :service, :message, :enum, :option]

    for field <- fields do
      IO.inspect(get_vals(ast, field), label: Atom.to_string(field), limit: 50)
    end

    IO.inspect(Keyword.drop(ast, fields), label: "rest", limit: 50)
  end

  defp get_vals(ast, key) do
    Enum.filter(ast, fn {k, _val} -> k == key end)
  end
end
