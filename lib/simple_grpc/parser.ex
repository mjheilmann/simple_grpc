defmodule SimpleGrpc.Parser do
  @moduledoc """
  Parses proto files and returns the AST.
  """

  @doc """
  Parses a proto file path and returns `{:ok, version, ast}` or `{:error, reason}`.
  """
  def parse(service_filepath) do
    case File.read(service_filepath) do
      {:ok, contents} ->
        with {:ok, version} <- version(contents),
             {:ok, ast} <- parse_contents(version, contents) do
          {:ok, version, ast}
        end

      {:error, reason} ->
        {:error, "could not read file #{service_filepath}: #{:file.format_error(reason)}"}
    end
  end

  @doc """
  Parses a proto file path and returns `{version, ast}` or throws.
  """
  def parse!(service_filepath) do
    case parse(service_filepath) do
      {:ok, version, ast} ->
        {version, ast}

      {:error, reason} ->
        throw({:error, "Failed to parse #{inspect(service_filepath)}: #{reason}"})
    end
  end

  defp version(proto_contents) do
    cond do
      Regex.match?(~r/.*syntax\s*=\s*["']proto3["'].*/, proto_contents) ->
        {:ok, :proto3}

      Regex.match?(~r/.*syntax\s*=\s*["']proto2["'].*/, proto_contents) ->
        {:error, "invalid proto file, proto2 syntax is not supported"}

      Regex.match?(
        ~r/.*(?:syntax\s*=\s*["']editions["']|edition\s*=\s*["'][^"']+["']).*/,
        proto_contents
      ) ->
        {:error, "invalid proto file, protobuf editions are not supported"}

      captures = Regex.named_captures(~r/.*syntax\s*=\s*["'](?<ver>[^"']+)["'].*/, proto_contents) ->
        {:error, "invalid proto file, unsupported syntax: #{inspect(captures["ver"])}"}

      true ->
        {:error, "invalid proto file, no syntax declaration"}
    end
  end

  defp parse_contents(:proto3, contents) do
    case SimpleGrpc.Parser.Proto3.parse(contents) do
      {:ok, ast, "", %{}, _, _} ->
        {:ok, ast}

      err ->
        {:error, "could not parse file: #{inspect(err)}"}
    end
  end
end
