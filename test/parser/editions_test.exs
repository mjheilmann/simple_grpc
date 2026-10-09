defmodule SimpleGrpc.Parser.EditionsTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Parser.Editions

  describe "Editions parser AST generation" do
    test "parses edition declaration" do
      proto = """
      edition = "2023";
      package test.editions;
      """

      assert {:ok, ast, "", %{}, _, _} = Editions.parse(proto)
      assert {:edition, "2023"} in ast
      assert {:package, "test.editions"} in ast
    end

    test "parses syntax = 'editions' format" do
      proto = """
      syntax = "editions";
      package test.editions;
      """

      assert {:ok, ast, "", %{}, _, _} = Editions.parse(proto)
      assert {:edition, "2023"} in ast
    end

    test "parses file-level feature options" do
      proto = """
      edition = "2023";
      package test.editions;

      option features.field_presence = IMPLICIT;
      option (features.repeated_field_encoding) = EXPANDED;
      """

      assert {:ok, ast, "", %{}, _, _} = Editions.parse(proto)
      assert {:option, ["features.field_presence", "IMPLICIT"]} in ast
      assert {:option, ["(features.repeated_field_encoding)", "EXPANDED"]} in ast
    end

    test "parses messages with edition fields and options" do
      proto = """
      edition = "2023";

      message UserProfile {
        string user_id = 1;
        int32 age = 2 [features.field_presence = LEGACY_REQUIRED];
        repeated string tags = 3 [features.repeated_field_encoding = EXPANDED];
        map<string, int64> metadata = 4;
      }
      """

      assert {:ok, ast, "", %{}, _, _} = Editions.parse(proto)
      assert [{:edition, "2023"}, {:message, ["UserProfile" | msg_items]}] = ast

      fields =
        Enum.flat_map(msg_items, fn
          {:field, _} = f -> [SimpleGrpc.Schema.Field.parse(f)]
          {:map, _} = m -> [SimpleGrpc.Schema.Field.parse(m)]
          _ -> []
        end)

      field_names = Enum.map(fields, & &1.name)

      assert :user_id in field_names
      assert :age in field_names
      assert :tags in field_names
      assert :metadata in field_names
    end

    test "parses enums, services, and extensions in editions" do
      proto = """
      edition = "2023";
      package company.service;

      enum Status {
        STATUS_UNSPECIFIED = 0;
        STATUS_ACTIVE = 1;
        STATUS_SUSPENDED = 2;
        option features.enum_type = CLOSED;
      }

      message BaseMessage {
        string id = 1;
        extensions 100 to 199;
      }

      extend BaseMessage {
        string extra_notes = 101;
      }

      service UserService {
        rpc GetProfile (BaseMessage) returns (BaseMessage);
      }
      """

      assert {:ok, ast, "", %{}, _, _} = Editions.parse(proto)
      assert {:edition, "2023"} in ast
      assert {:package, "company.service"} in ast

      assert Enum.any?(ast, fn
               {:enum, ["Status" | _]} -> true
               _ -> false
             end)

      assert Enum.any?(ast, fn
               {:message, ["BaseMessage" | _]} -> true
               _ -> false
             end)

      assert Enum.any?(ast, fn
               {:extend, ["BaseMessage" | _]} -> true
               _ -> false
             end)

      assert Enum.any?(ast, fn
               {:service, ["UserService" | _]} -> true
               _ -> false
             end)
    end

    test "supports edition 2024" do
      proto = """
      edition = "2024";
      package test.editions2024;

      message NewModel {
        string name = 1;
      }
      """

      tmp_path = Path.join(System.tmp_dir!(), "edition_2024.proto")
      File.write!(tmp_path, proto)

      on_exit(fn -> File.rm(tmp_path) end)

      assert {:ok, {:editions, "2024"}, _ast} = SimpleGrpc.Parser.parse(tmp_path)
    end

    test "returns clear error when an unsupported future edition is specified" do
      proto = """
      edition = "2099";
      package future.proto;

      message QuantumData {
        string state = 1;
      }
      """

      tmp_path = Path.join(System.tmp_dir!(), "edition_2099.proto")
      File.write!(tmp_path, proto)

      on_exit(fn -> File.rm(tmp_path) end)

      assert {:error, msg} = SimpleGrpc.Parser.parse(tmp_path)
      assert msg =~ "unsupported protobuf edition: \"2099\""
      assert msg =~ "supported editions: 2023, 2024"

      # Also test Schema.from_string
      assert {:error, str_msg} = SimpleGrpc.Schema.from_string(proto)
      assert str_msg =~ "unsupported protobuf edition: \"2099\""
    end
  end
end
