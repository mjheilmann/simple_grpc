defmodule SimpleGrpc.Server.Messages do
  @moduledoc false
  # Decides which struct module each protobuf message of a server's file set gets, and
  # resolves RPC input/output type names to those modules. Used by `use SimpleGrpc.Server`.
  #
  # A message `pkg.sub.Name` becomes `<ServerModule>.Pkg.Sub.Name` (each package segment
  # camelized), so equally named messages in different packages never collide and a
  # module name never depends on which other files happen to be in the set.
  #
  # Only messages the service can reach get a struct: the RPC input/output types and,
  # transitively, every message type their fields mention. The rest of the listed files
  # (other services' messages, a glob's leftovers) is ignored, so it can neither bloat the
  # server nor trigger module name collisions.

  alias SimpleGrpc.Schema
  alias SimpleGrpc.Schema.Field
  alias SimpleGrpc.Schema.WellKnownTypes
  alias SimpleGrpc.Server.Message

  defmodule Def do
    @moduledoc false
    defstruct [:message, :fqn, :package, :namespace, :module]
  end

  @doc """
  Collects the top-level messages reachable from the `rpcs` of a service in
  `service_package` and assigns each its module. Raises a `CompileError` if an RPC type is
  unknown, or if two different reachable messages would get the same module name.

  Reachability is tracked per top-level message: a nested message is generated inside its
  parent, so reaching one keeps the whole parent.
  """
  def collect!(entries, server_module, rpcs, service_package, env) do
    all = Enum.flat_map(entries, &entry_defs(&1, server_module))
    index = index(all)

    roots =
      Enum.flat_map(rpcs, fn rpc ->
        for type <- [rpc.input, rpc.output],
            fqn = resolve!(type, service_package, index, rpc.name, env),
            do: fqn
      end)

    defs = prune(all, roots)
    check_collisions!(defs, env)
    defs
  end

  @doc """
  Maps fully-qualified message names (nested messages included) to their modules.
  """
  def index(defs) do
    Enum.reduce(defs, %{}, fn %Def{message: message, fqn: fqn, module: module}, acc ->
      index_message(message, fqn, module, acc)
    end)
  end

  @doc """
  The map behind `__messages__/0`: fully-qualified name to module for every top-level
  message, plus the short name for messages in the service's own package.
  """
  def messages_map(defs, service_package) do
    Map.new(defs, &{&1.fqn, &1.module})
    |> Map.merge(
      for(
        %Def{package: package, message: message, module: module} <- defs,
        package == service_package,
        into: %{},
        do: {message.name, module}
      )
    )
  end

  @doc """
  Resolves the type name `type` as written in an `rpc` of a service in `package`.

  Returns the struct module, or `nil` for a bundled well-known type (those stay plain
  maps). Raises a `CompileError` if the type is not part of the file set.
  """
  def type_module!(type, package, index, rpc_name, env) do
    case resolve!(type, package, index, rpc_name, env) do
      nil -> nil
      fqn -> Map.fetch!(index, fqn)
    end
  end

  # The fully-qualified name of the message `type` refers to, or nil for a bundled
  # well-known type.
  defp resolve!(type, package, index, rpc_name, env) do
    resolved =
      type
      |> candidates(package)
      |> Enum.find_value(fn candidate ->
        cond do
          Map.has_key?(index, candidate) -> {:ok, candidate}
          well_known?(candidate) -> {:ok, nil}
          true -> nil
        end
      end)

    case resolved do
      {:ok, fqn} ->
        fqn

      nil ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "rpc #{rpc_name}: type #{inspect(type)} is not defined in the listed files " <>
              "and is not a bundled well-known type"
    end
  end

  defp entry_defs(entry, server_module) do
    package = package(entry.ast)
    namespace = namespace(server_module, package)

    for {:message, [_name | _]} = ast <- entry.ast do
      message = Message.parse(ast)

      %Def{
        message: message,
        fqn: qualify(package, message.name),
        package: package,
        namespace: namespace,
        module: Module.concat(namespace, message.name)
      }
    end
  end

  # Keeps the top-level messages reachable from `roots` (fully-qualified names), in their
  # original order.
  defp prune(defs, roots) do
    owner =
      for def <- defs, fqn <- tree_fqns(def.message, def.fqn), into: %{}, do: {fqn, def.fqn}

    by_fqn = Map.new(defs, &{&1.fqn, &1})
    reached = reachable(roots, MapSet.new(), owner, by_fqn)

    Enum.filter(defs, &MapSet.member?(reached, &1.fqn))
  end

  defp reachable([], seen, _owner, _by_fqn), do: seen

  defp reachable([fqn | rest], seen, owner, by_fqn) do
    top = Map.fetch!(owner, fqn)

    if MapSet.member?(seen, top) do
      reachable(rest, seen, owner, by_fqn)
    else
      %Def{message: message, fqn: top_fqn} = Map.fetch!(by_fqn, top)
      reachable(references(message, top_fqn, owner) ++ rest, MapSet.put(seen, top), owner, by_fqn)
    end
  end

  # Fully-qualified names of the messages referenced by the fields of `message` and of
  # every message nested in it.
  defp references(message, fqn, owner) do
    own =
      for field <- message.fields,
          type <- field_types(field),
          is_binary(type),
          not scalar?(type),
          ref = lookup(type, fqn, owner),
          do: ref

    nested =
      Enum.flat_map(message.nested_messages, &references(&1, qualify(fqn, &1.name), owner))

    own ++ nested
  end

  defp field_types(%Field{kind: :oneof, fields: fields}),
    do: Enum.flat_map(fields, &field_types/1)

  defp field_types(%Field{kind: :map, key_type: key, val_type: value}), do: [key, value]
  defp field_types(%Field{type: type}), do: [type]

  # `scope` is the fully-qualified name of the message that declares the field.
  defp lookup(type, scope, owner) do
    type |> candidates(scope) |> Enum.find(&Map.has_key?(owner, &1))
  end

  defp tree_fqns(message, fqn) do
    [fqn | Enum.flat_map(message.nested_messages, &tree_fqns(&1, qualify(fqn, &1.name)))]
  end

  @scalars ~w(double float int32 int64 uint32 uint64 sint32 sint64 fixed32 fixed64
              sfixed32 sfixed64 bool string bytes)

  defp scalar?(type), do: type in @scalars

  defp index_message(message, fqn, module, acc) do
    acc = Map.put(acc, fqn, module)

    Enum.reduce(message.nested_messages, acc, fn nested, inner ->
      index_message(nested, qualify(fqn, nested.name), Module.concat(module, nested.name), inner)
    end)
  end

  # Protobuf scoping: a relative name is looked up from the innermost enclosing scope (a
  # message or the package) outwards; a leading dot means fully qualified.
  defp candidates(type, scope), do: Schema.scope_candidates(type, scope)

  defp check_collisions!(defs, env) do
    defs
    |> Enum.group_by(& &1.module)
    |> Enum.each(fn {module, same_module} ->
      case Enum.uniq(Enum.map(same_module, & &1.fqn)) do
        [_one] ->
          :ok

        fqns ->
          raise CompileError,
            file: env.file,
            line: env.line,
            description:
              "#{Enum.join(fqns, " and ")} would both be generated as #{inspect(module)}; " <>
                "rename a package or message so their module names differ"
      end
    end)
  end

  defp package(ast) do
    Enum.find_value(ast, "", fn
      {:package, package} when is_binary(package) -> package
      _ -> nil
    end)
  end

  defp namespace(server_module, package) do
    segments = package |> String.split(".", trim: true) |> Enum.map(&Macro.camelize/1)
    Module.concat([server_module | segments])
  end

  defp qualify("", name), do: name
  defp qualify(package, name), do: "#{package}.#{name}"

  defp well_known?("google.protobuf." <> _ = fqn) do
    Map.has_key?(WellKnownTypes.unified_schema().messages, fqn)
  end

  defp well_known?(_), do: false
end
