defmodule SimpleGrpc.WKTStructsTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.WKT

  describe "the library-owned structs" do
    test "Timestamp, Duration, FieldMask and Empty are structs of their own modules" do
      assert %WKT.Timestamp{seconds: nil, nanos: nil, __unknown_fields__: []} =
               struct(WKT.Timestamp)

      assert %WKT.Duration{} = struct(WKT.Duration)
      assert %WKT.FieldMask{paths: nil} = struct(WKT.FieldMask)
      assert %WKT.Empty{} = struct(WKT.Empty)
    end

    test "they know their schema and message name" do
      assert WKT.Timestamp.__message_name__() == "Timestamp"
      assert WKT.Timestamp.__schema__() == SimpleGrpc.Schema.WellKnownTypes.unified_schema()
      assert Enum.map(WKT.Timestamp.__fields__(), & &1.name) == [:seconds, :nanos]
    end

    test "new/1 builds them with proto3 defaults" do
      assert %WKT.Timestamp{seconds: 5, nanos: 0} = WKT.Timestamp.new(%{seconds: 5})
      assert %WKT.FieldMask{paths: ["a"]} = WKT.FieldMask.new(paths: ["a"])
    end

    test "struct_modules/0 names the fully-qualified messages that are structs" do
      assert WKT.struct_modules() == %{
               "google.protobuf.Timestamp" => WKT.Timestamp,
               "google.protobuf.Duration" => WKT.Duration,
               "google.protobuf.FieldMask" => WKT.FieldMask,
               "google.protobuf.Empty" => WKT.Empty
             }
    end
  end

  describe "the helpers build structs and accept structs or maps" do
    test "Timestamp" do
      dt = ~U[2023-11-14 22:13:20.500000Z]

      assert %WKT.Timestamp{seconds: 1_700_000_000, nanos: 500_000_000} =
               WKT.Timestamp.from_datetime(dt)

      assert %WKT.Timestamp{seconds: 7, nanos: 0} = WKT.Timestamp.from_unix_seconds(7)
      assert %WKT.Timestamp{} = WKT.Timestamp.now()

      assert {:ok, ^dt} =
               WKT.Timestamp.to_datetime(%WKT.Timestamp{
                 seconds: 1_700_000_000,
                 nanos: 500_000_000
               })

      assert {:ok, ^dt} = WKT.Timestamp.to_datetime(%{seconds: 1_700_000_000, nanos: 500_000_000})
    end

    test "Duration" do
      assert %WKT.Duration{seconds: 12, nanos: 750_000_000} = WKT.Duration.from_seconds(12.75)
      assert %WKT.Duration{seconds: 1, nanos: 500_000_000} = WKT.Duration.from_milliseconds(1500)
      assert WKT.Duration.to_milliseconds(%WKT.Duration{seconds: 1, nanos: 500_000_000}) == 1500
    end

    test "FieldMask and Empty" do
      assert %WKT.FieldMask{paths: ["a", "b"]} = WKT.FieldMask.from_paths([:a, "b"])
      assert WKT.FieldMask.to_paths(%WKT.FieldMask{paths: ["a"]}) == ["a"]
      assert %WKT.Empty{} = WKT.Empty.new()
    end
  end
end
