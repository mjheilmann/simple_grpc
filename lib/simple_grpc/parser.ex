defmodule SimpleGrpc.Parser do
  def parse!(service_filepath) do
    dirname = service_filepath |> Path.dirname() |> Path.expand()
    IO.inspect(dirname)

    contents = File.read!(service_filepath)

    with {:ok, version} <- version(contents),
         {:ok, service_name} <- service(contents),
         {:ok, ast} <- parse(version, contents) do
      {version, service_name, ast}
    end
  end

  defp version(proto_contents) do
    cond do
      Regex.match?(~r/.*syntax = "?proto3"?.*/, proto_contents) -> {:ok, :proto3}
      true -> {:error, "unrecognized proto version"}
    end
  end

  defp service(proto_contents) do
    case Regex.named_captures(~r/.*service (?<service_name>[a-zA-Z0-9_]+) {.*/, proto_contents) do
      %{"service_name" => service_name} -> {:ok, service_name}
      _ -> {:error, "service definition not found"}
    end
  end

  defp parse(:proto3, contents) do
    case SimpleGrpc.Parser.Proto3.parse(contents) do
      {:ok, ast, "", %{}, _, _} ->
        {:ok, ast}

      err ->
        IO.inspect(err, label: "parse error")
        {:error, "could not parse file"}
    end
  end
end
