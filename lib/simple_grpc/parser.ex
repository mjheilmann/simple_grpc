defmodule SimpleGrpc.Parser do
  @moduledoc """
  Parses proto files and returns the AST.
  """

  @doc """
  Parses a proto file path and returns `{:ok, version, service_name, ast}` or `{:error, reason}`.
  """
  def parse(service_filepath) do
    case File.read(service_filepath) do
      {:ok, contents} ->
        with {:ok, version} <- version(contents),
             {:ok, service_name} <- service(contents),
             {:ok, ast} <- parse_contents(version, contents) do
          {:ok, version, service_name, ast}
        end

      {:error, reason} ->
        {:error, "could not read file #{service_filepath}: #{:file.format_error(reason)}"}
    end
  end

  @doc """
  Parses a proto file path and returns `{version, service_name, ast}` or raises `ArgumentError`.
  """
  def parse!(service_filepath) do
    case parse(service_filepath) do
      {:ok, version, service_name, ast} ->
        {version, service_name, ast}

      {:error, reason} ->
        raise ArgumentError, "Failed to parse #{inspect(service_filepath)}: #{reason}"
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

  defp parse_contents(:proto3, contents) do
    case SimpleGrpc.Parser.Proto3.parse(contents) do
      {:ok, ast, "", %{}, _, _} ->
        {:ok, ast}

      err ->
        {:error, "could not parse file: #{inspect(err)}"}
    end
  end
end

