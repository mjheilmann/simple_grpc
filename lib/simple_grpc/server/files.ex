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

  @glob_chars ~r/[*?\[{]/

  @doc """
  Resolves the proto paths requested by the `use` options.

  Returns `{paths, glob_entries}`. `files:` may be any expression that evaluates to a
  list of strings at compile time (a literal, a module attribute, `Path.wildcard/1`,
  ...). Entries containing glob characters are expanded (see `expand/1`).
  `glob_entries` is the `files:` list as written when it contains at least one glob,
  so the server can later tell whether the expansion changed; otherwise it is `nil`.

  `server_file:` is shorthand for a one-element `files:` list and is never globbed.
  """
  def resolve!(opts, env) do
    case {Keyword.fetch(opts, :files), Keyword.fetch(opts, :server_file)} do
      {{:ok, _}, {:ok, _}} ->
        error!(env, "pass either files: or server_file:, not both")

      {{:ok, files_ast}, :error} ->
        {files, _binding} = Code.eval_quoted(files_ast, [], env)
        entries = validate_paths!(files, env)
        {expand!(entries, env), if(Enum.any?(entries, &glob?/1), do: entries)}

      {:error, {:ok, server_file}} ->
        {validate_paths!([server_file], env), nil}

      {:error, :error} ->
        error!(env, "use SimpleGrpc.Server requires files: (a list of .proto file paths)")
    end
  end

  @doc """
  Expands `files:` entries into the list of paths to load.

  Plain entries are kept as written. A glob entry is replaced by the regular `.proto`
  files it matches, sorted. Order follows the list and each file appears once, at its
  first occurrence. Never raises, so a running server can use it to check whether the
  expansion still matches what it was compiled with.
  """
  def expand(entries) do
    entries |> Enum.flat_map(&expand_entry/1) |> Enum.uniq()
  end

  defp expand!(entries, env) do
    for entry <- entries, glob?(entry), expand_entry(entry) == [] do
      error!(env, "files: pattern #{inspect(entry)} matched no .proto files")
    end

    expand(entries)
  end

  defp expand_entry(entry) do
    if glob?(entry) do
      entry
      |> Path.wildcard()
      |> Enum.filter(&(File.regular?(&1) and Path.extname(&1) == ".proto"))
      |> Enum.sort()
    else
      [entry]
    end
  end

  defp glob?(entry), do: String.match?(entry, @glob_chars)

  @doc """
  Parses every listed file and validates the set: all imports must be bundled
  well-known types or match exactly one listed file, and no fully-qualified name may
  be defined by more than one file.

  A listed copy of a bundled file (a vendored `google/protobuf/timestamp.proto`) is
  dropped when it defines the same messages and enums as the bundled one, and is an
  error when it differs: the bundled definitions are what other gRPC implementations
  speak.
  """
  def load!(paths, env) do
    entries = paths |> Enum.map(&load_entry!(&1, env)) |> drop_bundled_copies!(env)

    check_imports!(entries, env)
    check_duplicate_definitions!(entries, env)

    entries
  end

  @doc """
  Merges the per-file schemas into one. `files` keeps the paths exactly as listed so
  absolute build-machine paths never end up in compiled modules.

  `struct_modules` maps fully-qualified message names to the struct module they decode into.
  """
  def merged_schema(entries, paths, struct_modules \\ %{}) do
    schema =
      (well_known_schemas(entries) ++ Enum.map(entries, & &1.schema))
      |> Enum.map(&%{&1 | struct_modules: struct_modules})
      |> Schema.merge()

    %{schema | files: paths}
  end

  # The bundled schemas of the well-known types the files import, directly or through
  # other well-known types. They come first so a listed message that happens to share a
  # short name with one of them (a `Duration` of your own) keeps that alias.
  defp well_known_schemas(entries) do
    entries
    |> Enum.flat_map(& &1.schema.imports)
    |> well_known_imports(MapSet.new())
    |> Enum.sort()
    |> Enum.map(&WellKnownTypes.schema_for/1)
  end

  defp well_known_imports([], seen), do: seen

  defp well_known_imports([import | rest], seen) do
    case WellKnownTypes.schema_for(import) do
      nil ->
        well_known_imports(rest, seen)

      _ when is_map_key(seen, import) ->
        well_known_imports(rest, seen)

      schema ->
        well_known_imports(schema.imports ++ rest, MapSet.put(seen, import))
    end
  end

  @doc """
  Picks the entry and service name this server implements.

  With `service:` it must exist in the files. Without it, the files must define
  exactly one service.
  """
  def pick_service!(entries, requested, env) do
    candidates =
      for entry <- entries,
          {:service, [name | _]} <- entry.ast,
          is_binary(name),
          do: {entry, name}

    names = Enum.map(candidates, &elem(&1, 1))

    case {requested, candidates} do
      {nil, [only]} ->
        only

      {nil, []} ->
        error!(env, "no service definition found in #{inspect(Enum.map(entries, & &1.path))}")

      {nil, _several} ->
        error!(
          env,
          "the files define several services (#{Enum.join(names, ", ")}); " <>
            "set service: to choose which one this module implements"
        )

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

  defp drop_bundled_copies!(entries, env) do
    Enum.reject(entries, fn entry ->
      case bundled_counterpart(entry.path) do
        nil ->
          false

        {import, bundled} ->
          case differences(entry.schema, bundled) do
            [] ->
              true

            names ->
              error!(
                env,
                "#{entry.path} defines #{Enum.join(names, ", ")} differently from the bundled " <>
                  "#{import}. Remove it from files: to use the bundled definition."
              )
          end
      end
    end)
  end

  defp bundled_counterpart(path) do
    Enum.find_value(WellKnownTypes.all_schemas(), fn {import, schema} ->
      if import_matches?(path, import), do: {import, schema}
    end)
  end

  # Fully-qualified names whose definition differs between the two schemas.
  defp differences(%Schema{} = listed, %Schema{} = bundled) do
    ours = signatures(listed)
    theirs = signatures(bundled)

    (Map.keys(ours) ++ Map.keys(theirs))
    |> Enum.uniq()
    |> Enum.filter(&(Map.get(ours, &1) != Map.get(theirs, &1)))
    |> Enum.sort()
  end

  # Fully-qualified name to a normalized description of the definition (field numbers,
  # names, types and cardinality; comments and options do not matter).
  defp signatures(%Schema{package: package} = schema) do
    messages =
      for message <- unique_by_full_name(schema.messages), into: %{} do
        fields =
          message.fields
          |> Enum.flat_map(fn
            %{kind: :oneof, fields: members} -> members
            field -> [field]
          end)
          |> Enum.map(fn field ->
            {field.number, field.name, field.kind, field.prefix,
             normalize_type(field.type, package), normalize_type(field.key_type, package),
             normalize_type(field.val_type, package)}
          end)
          |> Enum.sort()

        {full_name(message), {:message, fields}}
      end

    enums =
      for enum <- unique_by_full_name(schema.enums), into: %{} do
        {full_name(enum), {:enum, enum.fields |> Enum.to_list() |> Enum.sort()}}
      end

    Map.merge(messages, enums)
  end

  defp normalize_type(nil, _package), do: nil

  defp normalize_type(type, package) do
    type |> to_string() |> String.trim_leading(".") |> String.replace_prefix("#{package}.", "")
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
