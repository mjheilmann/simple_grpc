defmodule SimpleGrpc.EnumScopeTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Protobuf

  # Enums that share a local name must resolve from the scope where they are referenced:
  # `Type` inside `Title` is `Title.Type`, inside `Program` it is `Program.Type`, and `Kind`
  # is a different enum in each package.
  defmodule ScopedServer do
    use SimpleGrpc.Server,
      files: [
        "test/fixtures/protos/enum_scope/enum_scope.proto",
        "test/fixtures/protos/enum_scope/other.proto"
      ]

    def get(_req, _ctx), do: {:ok, %{}}
    def plain(req, _ctx), do: {:ok, req}
    def hold(req, _ctx), do: {:ok, req}
  end

  setup_all do
    schema = ScopedServer.__schema__()
    assert schema.compiled_module
    %{codecs: [compiled: schema, interpreted: %{schema | compiled_module: nil}]}
  end

  for codec <- [:compiled, :interpreted] do
    describe "#{codec} codec" do
      test "same-named nested enums encode with their own values", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        # `Title.Type.FULL` and `Program.Type.FEATURE_FILM` are both 1; `SERIES` is 7.
        assert Protobuf.encode!("enumscope.Title", %{type: :FULL}, schema) == <<0x08, 1>>
        assert Protobuf.encode!("enumscope.Title", %{type: :REDUCED}, schema) == <<0x08, 2>>
        assert Protobuf.encode!("enumscope.Program", %{prog_type: :SERIES}, schema) == <<0x08, 7>>

        assert Protobuf.encode!("enumscope.Program", %{prog_type: :FEATURE_FILM}, schema) ==
                 <<0x08, 1>>
      end

      test "the same wire value decodes to the enum of the field's own scope", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert %{type: :FULL} = Protobuf.decode!("enumscope.Title", <<0x08, 1>>, schema)

        assert %{prog_type: :FEATURE_FILM} =
                 Protobuf.decode!("enumscope.Program", <<0x08, 1>>, schema)
      end

      test "map and qualified enum references resolve in scope", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        data = %{
          prog_type: :SERIES,
          by_name: %{"a" => :SERIES},
          title_type: :REDUCED,
          title: %{type: :FULL}
        }

        decoded =
          "enumscope.Program"
          |> Protobuf.encode!(data, schema)
          |> then(&Protobuf.decode!("enumscope.Program", &1, schema))

        assert decoded.prog_type == :SERIES
        assert decoded.by_name == %{"a" => :SERIES}
        # `Title.Type` written from inside `Program` is `Title`'s enum.
        assert decoded.title_type == :REDUCED
        assert decoded.title.type == :FULL
      end

      test "the same enum name in two packages resolves per package", %{codecs: codecs} do
        schema = codecs[unquote(codec)]

        assert Protobuf.encode!("enumscope.Plain", %{kind: :KIND_ALPHA}, schema) == <<0x08, 1>>
        assert Protobuf.encode!("enumother.Holder", %{kind: :OTHER_BETA}, schema) == <<0x08, 1>>

        assert %{kind: :KIND_ALPHA} = Protobuf.decode!("enumscope.Plain", <<0x08, 1>>, schema)
        assert %{kind: :OTHER_BETA} = Protobuf.decode!("enumother.Holder", <<0x08, 1>>, schema)
      end
    end
  end

  test "an enum value from another scope is rejected" do
    schema = ScopedServer.__schema__()

    assert {:error, _, _} = Protobuf.encode("enumscope.Title", %{type: :SERIES}, schema)
    assert {:error, _, _} = Protobuf.encode("enumscope.Plain", %{kind: :OTHER_BETA}, schema)
  end
end
