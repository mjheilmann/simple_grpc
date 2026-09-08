defmodule SimpleGrpc.Server.Field do
  @moduledoc """
  Backwards compatibility module aliasing SimpleGrpc.Schema.Field.
  """
  alias SimpleGrpc.Schema.Field, as: SchemaField

  defstruct [
    :name,
    :type,
    :number,
    :prefix,
    :deprecated,
    :kind,
    :key_type,
    :val_type,
    :fields,
    options: %{}
  ]

  @type t :: SchemaField.t()

  defdelegate parse(ast), to: SchemaField
end
