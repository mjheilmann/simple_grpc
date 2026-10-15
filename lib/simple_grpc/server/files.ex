defmodule SimpleGrpc.Server.Files do
  @moduledoc false
  # Compile-time loading and validation of the explicit `.proto` file list given to
  # `use SimpleGrpc.Server`. Every function takes the caller's `Macro.Env` so problems
  # are reported as `CompileError`s pointing at the `use` line.

  alias SimpleGrpc.Parser
  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.WellKnownTypes

  defmodule Entry do
    @moduledoc false
    defstruct [:path, :syntax, :ast, :schema]
  end

  @doc """
  Returns the list of proto paths requested by the `use` options.

  `files:` may be any expression that evaluates to a list of strings at compile time
  (a literal, a module attribute, `Path.wildcard/1`, ...). `server_file:` is shorthand
  for a one-element `files:` list.
  """
  def paths!(opts, env) do
    case {Keyword.fetch(opts, :files), Keyword.fetch(opts, :server_file)} do
      {{:ok, _}, {:ok, _}} ->
        error!(env, "pass either files: or server_file:, not both")

      {{:ok, files_ast}, :error} ->
        {files, _binding} = Code.eval_quoted(files_ast, [], env)
        validate_paths!(files, env)

      {:error, {:ok, server_file}} ->
        validate_paths!([server_file], env)

      {:error, :error} ->
        error!(env, "use SimpleGrpc.Server requires files: (a list of .proto file paths)")
    end
  end

  @doc """
  Parses every listed file and validates the set: all imports must be bundled
  well-known types or match exactly one listed file, and no fully-qualified name may
  be defined by more than one file.
  """
  def load!(paths, env) do
    entries = Enum.map(paths, &load_entry!(&1, env))

    check_imports!(entries, env)
    check_duplicate_definitions!(entries, env)

    entries
  end

  @doc """
  Merges the per-file schemas into one. `files` keeps the paths exactly as listed so
  absolute build-machine paths never end up in compiled modules.
  """
  def merged_schema(entries, paths) do
    schema = entries |> Enum.map(& &1.schema) |> Schema.merge()
    %{schema | files: paths}
  end

  @doc """
  Picks the entry and service name this server implements.

  With `service:` it must exist in the files. Without it, the first service in
  source order (listed file order, then position in the file) is used.
  """
  def pick_service!(entries, requested, env) do
    candidates =
      for entry <- entries,
          {:service, [name | _]} <- entry.ast,
          is_binary(name),
          do: {entry, name}

    names = Enum.map(candidates, &elem(&1, 1))

    case {requested, candidates} do
      {nil, [first | _]} ->
        first

      {nil, []} ->
        error!(env, "no service definition found in #{inspect(Enum.map(entries, & &1.path))}")

      {name, _} ->
        case Enum.find(candidates, fn {_entry, service_name} -> service_name == name end) do
          nil ->
            error!(
              env,
              "service #{inspect(name)} not found in the listed files " <>
                "(available: #{Enum.join(names, ", ")})"
            )

          candidate ->
            candidate
        end
    end
  end

  defp validate_paths!(paths, env) do
    if is_list(paths) and paths != [] and Enum.all?(paths, &is_binary/1) do
      paths
    else
      error!(env, "files: must be a non-empty list of .proto file paths, got: #{inspect(paths)}")
    end
  end

  defp load_entry!(path, env) do
    case Parser.parse(path) do
      {:ok, {:editions, edition}, ast} ->
        schema = Schema.from_ast(ast, syntax: :editions, edition: edition)
        %Entry{path: path, syntax: :editions, ast: ast, schema: %{schema | files: [path]}}

      {:ok, syntax, ast} ->
        schema = Schema.from_ast(ast, syntax: syntax)
        %Entry{path: path, syntax: syntax, ast: ast, schema: %{schema | files: [path]}}

      {:error, reason} ->
        error!(env, "failed to parse #{inspect(path)}: #{reason}")
    end
  end

  defp check_imports!(entries, env) do
    for entry <- entries, import <- entry.schema.imports do
      resolve_import!(entry, import, entries, env)
    end

    :ok
  end

  defp resolve_import!(entry, import, entries, env) do
    if WellKnownTypes.schema_for(import) do
      :ok
    else
      case Enum.filter(entries, &import_matches?(&1.path, import)) do
        [_one] ->
          :ok

        [] ->
          error!(
            env,
            "#{entry.path}: import #{inspect(import)} is not a bundled well-known type " <>
              "and does not match any file in files:. Add the file that provides it to files:."
          )

        many ->
          error!(
            env,
            "#{entry.path}: import #{inspect(import)} is ambiguous, it matches " <>
              "#{Enum.map_join(many, ", ", & &1.path)}. Remove the extra file(s) from files:."
          )
      end
    end
  end

  # An import string names a file by the tail of its path ("a/b.proto" matches
  # "priv/protos/a/b.proto" but not "priv/protos/xa/b.proto").
  defp import_matches?(path, import) do
    path = String.trim_leading(path, "./")
    path == import or String.ends_with?(path, "/" <> import)
  end

  defp check_duplicate_definitions!(entries, env) do
    entries
    |> Enum.flat_map(fn entry ->
      for fqn <- defined_names(entry.schema), do: {fqn, entry.path}
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.sort()
    |> Enum.each(fn {fqn, paths} ->
      case Enum.uniq(paths) do
        [_one] ->
          :ok

        many ->
          error!(
            env,
            "#{fqn} is defined in more than one listed file: #{Enum.join(many, ", ")}"
          )
      end
    end)
  end

  defp defined_names(%Schema{} = schema) do
    [schema.messages, schema.enums, schema.services]
    |> Enum.flat_map(&unique_by_full_name/1)
    |> Enum.map(&full_name/1)
    |> Enum.uniq()
  end

  defp unique_by_full_name(definitions) do
    definitions |> Map.values() |> Enum.uniq_by(&full_name/1)
  end

  defp full_name(definition), do: Map.get(definition, :full_name) || definition.name

  defp error!(env, message) do
    raise CompileError, file: env.file, line: env.line, description: message
  end
end
