defmodule SimpleGrpc.WKT do
  @moduledoc """
  High-level helpers and coercers for Google Well-Known Types (WKT).

  Supports:
  - `google.protobuf.Timestamp`
  - `google.protobuf.Duration`
  - `google.protobuf.Struct`, `google.protobuf.Value`, `google.protobuf.ListValue`
  - `google.protobuf.Any`
  - `google.protobuf.FieldMask`
  - `google.protobuf.Empty`
  """

  require SimpleGrpc.Server.Message

  @struct_messages ~w(Timestamp Duration FieldMask Empty)

  @doc """
  The schema of all bundled well-known types. It describes the structs of this module.
  """
  def __schema__, do: SimpleGrpc.Schema.WellKnownTypes.unified_schema()

  @doc """
  Maps the fully-qualified names of the well-known messages that are structs to their
  modules. Servers decode these types into these structs; every other well-known type
  (`Any`, `Struct`, `Value`, the wrappers, ...) stays a plain map.
  """
  def struct_modules do
    Map.new(@struct_messages, &{"google.protobuf." <> &1, Module.concat(__MODULE__, &1)})
  end

  # ===========================================================================
  # Timestamp
  # ===========================================================================

  defmodule Timestamp do
    @moduledoc """
    `google.protobuf.Timestamp` struct and helpers.
    """

    SimpleGrpc.Server.Message.define_bundled_struct("google.protobuf.Timestamp")

    @doc """
    Returns a Timestamp representing the current UTC time.
    """
    def now do
      from_datetime(DateTime.utc_now())
    end

    @doc """
    Converts an Elixir `DateTime` or `NaiveDateTime` to a protobuf Timestamp map.
    """
    def from_datetime(%DateTime{} = dt) do
      unix_micro = DateTime.to_unix(dt, :microsecond)
      seconds = div(unix_micro, 1_000_000)
      micro = rem(unix_micro, 1_000_000)

      {_clean, nanos} =
        if micro < 0,
          do: {micro + 1_000_000, (micro + 1_000_000) * 1_000},
          else: {micro, micro * 1_000}

      adjusted_seconds = if micro < 0, do: seconds - 1, else: seconds
      %__MODULE__{seconds: adjusted_seconds, nanos: nanos}
    end

    def from_datetime(%NaiveDateTime{} = ndt) do
      {:ok, dt} = DateTime.from_naive(ndt, "Etc/UTC")
      from_datetime(dt)
    end

    @doc """
    Converts a protobuf Timestamp map (`%{seconds: s, nanos: n}`) to an Elixir `DateTime`.
    """
    def to_datetime(%{seconds: seconds, nanos: nanos}) do
      unix_micro = (seconds || 0) * 1_000_000 + div(nanos || 0, 1_000)
      DateTime.from_unix(unix_micro, :microsecond)
    end

    def to_datetime(%{"seconds" => seconds, "nanos" => nanos}) do
      to_datetime(%{seconds: seconds, nanos: nanos})
    end

    def to_datetime(nil), do: {:error, :nil_timestamp}

    @doc """
    Converts a protobuf Timestamp map to `DateTime`, raising on error.
    """
    def to_datetime!(ts) do
      case to_datetime(ts) do
        {:ok, dt} ->
          dt

        {:error, reason} ->
          raise ArgumentError, "Invalid Timestamp #{inspect(ts)}: #{inspect(reason)}"
      end
    end

    @doc """
    Converts unix seconds (integer or float) to a protobuf Timestamp map.
    """
    def from_unix_seconds(sec) when is_integer(sec) do
      %__MODULE__{seconds: sec, nanos: 0}
    end

    def from_unix_seconds(sec) when is_float(sec) do
      s = trunc(sec)
      rem_sec = sec - s
      nanos = trunc(rem_sec * 1_000_000_000)
      %__MODULE__{seconds: s, nanos: nanos}
    end

    @doc """
    Converts a protobuf Timestamp map to unix seconds as float.
    """
    def to_unix_seconds(%{seconds: s, nanos: n}) do
      (s || 0) + (n || 0) / 1_000_000_000.0
    end

    def to_unix_seconds(%{"seconds" => s, "nanos" => n}) do
      to_unix_seconds(%{seconds: s, nanos: n})
    end
  end

  # ===========================================================================
  # Duration
  # ===========================================================================

  defmodule Duration do
    @moduledoc """
    `google.protobuf.Duration` struct and helpers.
    """

    SimpleGrpc.Server.Message.define_bundled_struct("google.protobuf.Duration")

    @doc """
    Converts seconds (integer or float) to a `google.protobuf.Duration`.
    """
    def from_seconds(sec) when is_integer(sec) do
      %__MODULE__{seconds: sec, nanos: 0}
    end

    def from_seconds(sec) when is_float(sec) do
      s = trunc(sec)
      rem_sec = sec - s
      nanos = trunc(rem_sec * 1_000_000_000)
      %__MODULE__{seconds: s, nanos: nanos}
    end

    @doc """
    Converts a protobuf Duration map to seconds as float.
    """
    def to_seconds(%{seconds: s, nanos: n}) do
      (s || 0) + (n || 0) / 1_000_000_000.0
    end

    def to_seconds(%{"seconds" => s, "nanos" => n}) do
      to_seconds(%{seconds: s, nanos: n})
    end

    @doc """
    Converts integer milliseconds to a `google.protobuf.Duration`.
    """
    def from_milliseconds(ms) when is_integer(ms) do
      %__MODULE__{seconds: div(ms, 1000), nanos: rem(ms, 1000) * 1_000_000}
    end

    @doc """
    Converts a protobuf Duration map to integer milliseconds.
    """
    def to_milliseconds(%{seconds: s, nanos: n}) do
      (s || 0) * 1000 + div(n || 0, 1_000_000)
    end

    def to_milliseconds(%{"seconds" => s, "nanos" => n}) do
      to_milliseconds(%{seconds: s, nanos: n})
    end
  end

  # ===========================================================================
  # Struct & Value
  # ===========================================================================

  defmodule Struct do
    @moduledoc """
    Helpers for `google.protobuf.Struct`, `Value`, and `ListValue`.
    """

    @doc """
    Converts an arbitrary Elixir map/list/primitive into a `google.protobuf.Struct` map structure.
    """
    def from_elixir(map) when is_map(map) do
      fields =
        Enum.into(map, %{}, fn {k, v} ->
          {to_string(k), from_elixir_value(v)}
        end)

      %{fields: fields}
    end

    @doc """
    Converts an Elixir term to a `google.protobuf.Value` map.
    """
    def from_elixir_value(nil) do
      %{null_value: 0}
    end

    def from_elixir_value(num) when is_number(num) do
      %{number_value: num * 1.0}
    end

    def from_elixir_value(str) when is_binary(str) do
      %{string_value: str}
    end

    def from_elixir_value(bool) when is_boolean(bool) do
      %{bool_value: bool}
    end

    def from_elixir_value(atom) when is_atom(atom) do
      %{string_value: Atom.to_string(atom)}
    end

    def from_elixir_value(list) when is_list(list) do
      %{list_value: %{values: Enum.map(list, &from_elixir_value/1)}}
    end

    def from_elixir_value(map) when is_map(map) do
      %{struct_value: from_elixir(map)}
    end

    @doc """
    Converts a `google.protobuf.Struct` or `Value` map into native Elixir maps, lists, and scalars.
    """
    def to_elixir(%{fields: fields}) when is_map(fields) do
      Enum.into(fields, %{}, fn {k, v} ->
        {k, to_elixir_value(v)}
      end)
    end

    def to_elixir(%{"fields" => fields}) when is_map(fields) do
      Enum.into(fields, %{}, fn {k, v} ->
        {k, to_elixir_value(v)}
      end)
    end

    def to_elixir(other) do
      to_elixir_value(other)
    end

    @doc """
    Converts a `google.protobuf.Value` map to a native Elixir term.
    """
    def to_elixir_value(%{null_value: _}), do: nil
    def to_elixir_value(%{"null_value" => _}), do: nil
    def to_elixir_value(%{number_value: num}) when not is_nil(num), do: num
    def to_elixir_value(%{"number_value" => num}) when not is_nil(num), do: num
    def to_elixir_value(%{string_value: str}) when not is_nil(str), do: str
    def to_elixir_value(%{"string_value" => str}) when not is_nil(str), do: str
    def to_elixir_value(%{bool_value: bool}) when not is_nil(bool), do: bool
    def to_elixir_value(%{"bool_value" => bool}) when not is_nil(bool), do: bool
    def to_elixir_value(%{struct_value: %{} = s}), do: to_elixir(s)
    def to_elixir_value(%{"struct_value" => %{} = s}), do: to_elixir(s)

    def to_elixir_value(%{list_value: %{values: list}}) when is_list(list),
      do: Enum.map(list, &to_elixir_value/1)

    def to_elixir_value(%{"list_value" => %{"values" => list}}) when is_list(list),
      do: Enum.map(list, &to_elixir_value/1)

    def to_elixir_value(%{kind: {_kind_name, val}}), do: to_elixir_value(val)
    def to_elixir_value(term), do: term
  end

  # ===========================================================================
  # Any
  # ===========================================================================

  defmodule Any do
    @moduledoc """
    Helpers for packing and unpacking `google.protobuf.Any`.
    """

    alias SimpleGrpc.Protobuf
    alias SimpleGrpc.Schema.WellKnownTypes

    @doc """
    Packs an Elixir message map into a `google.protobuf.Any` map.
    """
    def pack(data, message_type, schema \\ nil) do
      schema_ctx = schema || WellKnownTypes.unified_schema()
      type_str = to_string(message_type) |> String.trim_leading(".")

      type_url =
        if String.contains?(type_str, "/") do
          type_str
        else
          "type.googleapis.com/#{type_str}"
        end

      encoded_bytes = Protobuf.encode!(message_type, data, schema_ctx)
      %{type_url: type_url, value: encoded_bytes}
    end

    @doc """
    Unpacks a `google.protobuf.Any` map into an Elixir message map.
    """
    def unpack(msg, schema \\ nil)

    def unpack(%{type_url: type_url, value: value}, schema) do
      schema_ctx = schema || WellKnownTypes.unified_schema()
      type_name = extract_type_name(type_url)

      try do
        decoded = Protobuf.decode!(type_name, value, schema_ctx)
        {:ok, decoded}
      catch
        :throw, err -> {:error, err}
        _, err -> {:error, err}
      end
    end

    def unpack(%{"type_url" => type_url, "value" => value}, schema) do
      unpack(%{type_url: type_url, value: value}, schema)
    end

    @doc """
    Unpacks a `google.protobuf.Any` map, raising on error.
    """
    def unpack!(any_msg, schema \\ nil) do
      case unpack(any_msg, schema) do
        {:ok, decoded} ->
          decoded

        {:error, reason} ->
          raise ArgumentError, "Failed to unpack Any #{inspect(any_msg)}: #{inspect(reason)}"
      end
    end

    defp extract_type_name(type_url) when is_binary(type_url) do
      case String.split(type_url, "/", parts: 2) do
        [_prefix, name] -> name
        [name] -> name
      end
    end
  end

  # ===========================================================================
  # FieldMask
  # ===========================================================================

  defmodule FieldMask do
    @moduledoc """
    `google.protobuf.FieldMask` struct and helpers.
    """

    SimpleGrpc.Server.Message.define_bundled_struct("google.protobuf.FieldMask")

    @doc """
    Constructs a `google.protobuf.FieldMask` from a list of paths (strings or atoms).
    """
    def from_paths(paths) when is_list(paths) do
      %__MODULE__{paths: Enum.map(paths, &to_string/1)}
    end

    @doc """
    Extracts the list of paths from a `google.protobuf.FieldMask` map.
    """
    def to_paths(%{paths: paths}) when is_list(paths), do: paths
    def to_paths(%{"paths" => paths}) when is_list(paths), do: paths
    def to_paths(_), do: []

    @doc """
    Applies a field mask to copy only the specified fields from `source` into `target`.
    """
    def apply_mask(target, source, paths)
        when is_map(target) and is_map(source) and is_list(paths) do
      Enum.reduce(paths, target, fn path, acc ->
        path_segments = String.split(to_string(path), ".")
        copy_nested_path(acc, source, path_segments)
      end)
    end

    defp copy_nested_path(target, source, [key]) do
      atom_key = String.to_atom(key)

      val =
        cond do
          Map.has_key?(source, atom_key) -> Map.get(source, atom_key)
          Map.has_key?(source, key) -> Map.get(source, key)
          true -> nil
        end

      if Map.has_key?(target, atom_key) or not Map.has_key?(target, key) do
        Map.put(target, atom_key, val)
      else
        Map.put(target, key, val)
      end
    end

    defp copy_nested_path(target, source, [key | rest]) do
      atom_key = String.to_atom(key)

      src_child =
        cond do
          is_map(Map.get(source, atom_key)) -> Map.get(source, atom_key)
          is_map(Map.get(source, key)) -> Map.get(source, key)
          true -> %{}
        end

      target_child =
        cond do
          is_map(Map.get(target, atom_key)) -> Map.get(target, atom_key)
          is_map(Map.get(target, key)) -> Map.get(target, key)
          true -> %{}
        end

      updated_child = copy_nested_path(target_child, src_child, rest)

      if Map.has_key?(target, atom_key) or not Map.has_key?(target, key) do
        Map.put(target, atom_key, updated_child)
      else
        Map.put(target, key, updated_child)
      end
    end
  end

  # ===========================================================================
  # Empty
  # ===========================================================================

  defmodule Empty do
    @moduledoc """
    `google.protobuf.Empty` struct.
    """

    SimpleGrpc.Server.Message.define_bundled_struct("google.protobuf.Empty")
  end

  # Top-level delegation helpers for convenience
  defdelegate now(), to: Timestamp
  defdelegate from_datetime(dt), to: Timestamp
  defdelegate to_datetime(ts), to: Timestamp
  defdelegate to_datetime!(ts), to: Timestamp
  defdelegate pack_any(data, message_type, schema \\ nil), to: Any, as: :pack
  defdelegate unpack_any(any_msg, schema \\ nil), to: Any, as: :unpack
  defdelegate unpack_any!(any_msg, schema \\ nil), to: Any, as: :unpack!
  defdelegate to_struct(map), to: Struct, as: :from_elixir
  defdelegate from_struct(struct_msg), to: Struct, as: :to_elixir
end
