defmodule SimpleGrpc.WKTTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.WKT
  alias SimpleGrpc.Schema

  describe "Timestamp helpers" do
    test "converts DateTime <-> Timestamp map" do
      now = DateTime.utc_now()
      ts = WKT.Timestamp.from_datetime(now)

      assert is_integer(ts.seconds)
      assert is_integer(ts.nanos)
      assert ts.nanos >= 0 and ts.nanos < 1_000_000_000

      assert {:ok, recovered_dt} = WKT.Timestamp.to_datetime(ts)
      assert DateTime.diff(now, recovered_dt, :microsecond) == 0
    end

    test "converts unix seconds <-> Timestamp map" do
      ts = WKT.Timestamp.from_unix_seconds(1_700_000_000.5)
      assert ts.seconds == 1_700_000_000
      assert ts.nanos == 500_000_000

      sec = WKT.Timestamp.to_unix_seconds(ts)
      assert_in_delta sec, 1_700_000_000.5, 0.0001
    end
  end

  describe "Duration helpers" do
    test "converts seconds <-> Duration map" do
      dur = WKT.Duration.from_seconds(12.75)
      assert dur.seconds == 12
      assert dur.nanos == 750_000_000

      sec = WKT.Duration.to_seconds(dur)
      assert_in_delta sec, 12.75, 0.0001
    end

    test "converts milliseconds <-> Duration map" do
      dur = WKT.Duration.from_milliseconds(1500)
      assert dur.seconds == 1
      assert dur.nanos == 500_000_000

      ms = WKT.Duration.to_milliseconds(dur)
      assert ms == 1500
    end
  end

  describe "Struct & Value helpers" do
    test "converts arbitrary Elixir map to Struct and back" do
      input_map = %{
        "name" => "Alice",
        "age" => 30.0,
        "is_admin" => true,
        "null_val" => nil,
        "tags" => ["dev", "lead"],
        "profile" => %{"role" => "Architect"}
      }

      struct_msg = WKT.Struct.from_elixir(input_map)
      assert is_map(struct_msg.fields)

      elixir_result = WKT.Struct.to_elixir(struct_msg)
      assert elixir_result["name"] == "Alice"
      assert elixir_result["age"] == 30.0
      assert elixir_result["is_admin"] == true
      assert elixir_result["null_val"] == nil
      assert elixir_result["tags"] == ["dev", "lead"]
      assert elixir_result["profile"]["role"] == "Architect"
    end
  end

  describe "Any packing and unpacking" do
    test "packs and unpacks messages using WKT Any" do
      proto = """
      syntax = "proto3";
      package test.wkt;

      message UserData {
        string user_id = 1;
        int32 score = 2;
      }
      """

      assert {:ok, schema} = Schema.from_string(proto)

      user_data = %{user_id: "user-42", score: 999}
      any_msg = WKT.Any.pack(user_data, "test.wkt.UserData", schema)

      assert any_msg.type_url == "type.googleapis.com/test.wkt.UserData"
      assert is_binary(any_msg.value)

      assert {:ok, unpacked} = WKT.Any.unpack(any_msg, schema)
      assert unpacked.user_id == "user-42"
      assert unpacked.score == 999
    end
  end

  describe "FieldMask helpers" do
    test "converts paths and applies mask" do
      mask = WKT.FieldMask.from_paths(["name", "settings.theme"])
      assert mask.paths == ["name", "settings.theme"]
      assert WKT.FieldMask.to_paths(mask) == ["name", "settings.theme"]

      target = %{name: "Old", email: "old@example.com", settings: %{theme: "dark", lang: "en"}}
      source = %{name: "New", email: "new@example.com", settings: %{theme: "light", lang: "fr"}}

      updated = WKT.FieldMask.apply_mask(target, source, mask.paths)
      assert updated.name == "New"
      assert updated.email == "old@example.com"
      assert updated.settings.theme == "light"
      assert updated.settings.lang == "en"
    end
  end

  describe "Empty helper" do
    test "creates empty map" do
      assert WKT.Empty.new() == %{}
    end
  end
end
