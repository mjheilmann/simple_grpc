defmodule SimpleGrpc.Schema.Enum do
  @moduledoc """
  Represents a protobuf enum definition.
  """

  defstruct [
    :name,
    fields: [],
    int_to_atom: %{},
    atom_to_int: %{},
    str_to_int: %{},
    zero_atom: nil,
    options: %{}
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          fields: list({atom(), integer()}),
          int_to_atom: map(),
          atom_to_int: map(),
          str_to_int: map(),
          zero_atom: atom() | nil,
          options: map()
        }

  @doc """
  Parses an enum AST node into an Enum struct.
  """
  def parse({:enum, [name | items]}) when is_binary(name) do
    fields =
      Enum.flat_map(items, fn
        {:enum_field, [fname, fval | _]} -> [{String.to_atom(fname), fval}]
        _ -> []
      end)

    options =
      Enum.reduce(items, %{}, fn
        {:option, [k, v]}, acc -> Map.put(acc, k, v)
        _, acc -> acc
      end)

    int_to_atom =
      Enum.reduce(fields, %{}, fn {atom_name, int_val}, acc ->
        signed_32 = if int_val > 0x7FFFFFFF, do: int_val - 0x100000000, else: int_val

        acc
        |> Map.put_new(int_val, atom_name)
        |> Map.put_new(signed_32, atom_name)
      end)

    atom_to_int =
      Enum.reduce(fields, %{}, fn {atom_name, int_val}, acc ->
        Map.put_new(acc, atom_name, int_val)
      end)

    str_to_int =
      Enum.reduce(fields, %{}, fn {atom_name, int_val}, acc ->
        acc
        |> Map.put_new(Atom.to_string(atom_name), int_val)
        |> Map.put_new(String.upcase(Atom.to_string(atom_name)), int_val)
      end)

    zero_atom =
      Enum.find_value(fields, fn {k, v} -> if v == 0, do: k end)

    %__MODULE__{
      name: name,
      fields: fields,
      int_to_atom: int_to_atom,
      atom_to_int: atom_to_int,
      str_to_int: str_to_int,
      zero_atom: zero_atom,
      options: options
    }
  end
end
