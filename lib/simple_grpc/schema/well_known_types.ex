defmodule SimpleGrpc.Schema.WellKnownTypes do
  @moduledoc """
  Pre-parsed `%SimpleGrpc.Schema{}` metadata for Google Well-Known Types (WKT).
  Compiled into BEAM bytecode at build-time for zero runtime parsing overhead and
  zero external disk or dependency requirements.
  """

  alias SimpleGrpc.Schema

  @raw_protos %{
    "google/protobuf/timestamp.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message Timestamp {
      int64 seconds = 1;
      int32 nanos = 2;
    }
    """,
    "google/protobuf/duration.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message Duration {
      int64 seconds = 1;
      int32 nanos = 2;
    }
    """,
    "google/protobuf/empty.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message Empty {}
    """,
    "google/protobuf/any.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message Any {
      string type_url = 1;
      bytes value = 2;
    }
    """,
    "google/protobuf/struct.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message Struct {
      map<string, Value> fields = 1;
    }

    message Value {
      oneof kind {
        NullValue null_value = 1;
        double number_value = 2;
        string string_value = 3;
        bool bool_value = 4;
        Struct struct_value = 5;
        ListValue list_value = 6;
      }
    }

    enum NullValue {
      NULL_VALUE = 0;
    }

    message ListValue {
      repeated Value values = 1;
    }
    """,
    "google/protobuf/wrappers.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message DoubleValue { double value = 1; }
    message FloatValue { float value = 1; }
    message Int64Value { int64 value = 1; }
    message UInt64Value { uint64 value = 1; }
    message Int32Value { int32 value = 1; }
    message UInt32Value { uint32 value = 1; }
    message BoolValue { bool value = 1; }
    message StringValue { string value = 1; }
    message BytesValue { bytes value = 1; }
    """,
    "google/protobuf/field_mask.proto" => """
    syntax = "proto3";
    package google.protobuf;

    message FieldMask {
      repeated string paths = 1;
    }
    """,
    "google/rpc/status.proto" => """
    syntax = "proto3";
    package google.rpc;

    import "google/protobuf/any.proto";

    message Status {
      int32 code = 1;
      string message = 2;
      repeated google.protobuf.Any details = 3;
    }
    """,
    "google/rpc/error_details.proto" => """
    syntax = "proto3";
    package google.rpc;

    import "google/protobuf/duration.proto";

    message RetryInfo {
      google.protobuf.Duration retry_delay = 1;
    }

    message DebugInfo {
      repeated string stack_entries = 1;
      string detail = 2;
    }

    message QuotaFailure {
      message Violation {
        string subject = 1;
        string description = 2;
      }
      repeated QuotaFailure.Violation violations = 1;
    }

    message ErrorInfo {
      string reason = 1;
      string domain = 2;
      map<string, string> metadata = 3;
    }

    message PreconditionFailure {
      message Violation {
        string type = 1;
        string subject = 2;
        string description = 3;
      }
      repeated PreconditionFailure.Violation violations = 1;
    }

    message BadRequest {
      message FieldViolation {
        string field = 1;
        string description = 2;
      }
      repeated BadRequest.FieldViolation field_violations = 1;
    }

    message RequestInfo {
      string request_id = 1;
      string serving_data = 2;
    }

    message ResourceInfo {
      string resource_type = 1;
      string resource_name = 2;
      string owner = 3;
      string description = 4;
    }

    message Help {
      message Link {
        string description = 1;
        string url = 2;
      }
      repeated Help.Link links = 1;
    }

    message LocalizedMessage {
      string locale = 1;
      string message = 2;
    }
    """
  }

  # Parse each proto once at compile time into structured %SimpleGrpc.Schema{} ASTs
  @compiled_schemas Enum.into(@raw_protos, %{}, fn {path, proto_str} ->
                      {:ok, schema} = Schema.from_string(proto_str)
                      {path, schema}
                    end)

  @unified_schema Enum.reduce(Map.values(@compiled_schemas), %Schema{}, &Schema.merge/2)

  @doc """
  Returns the pre-parsed `%SimpleGrpc.Schema{}` struct for a Google Well-Known Type import path,
  or `nil` if not a recognized WKT.
  """
  def schema_for(import_path) when is_binary(import_path) do
    Map.get(@compiled_schemas, import_path)
  end

  @doc """
  Returns a merged `%SimpleGrpc.Schema{}` containing all Google Well-Known Types pre-parsed.
  """
  def all_schemas do
    @compiled_schemas
  end

  @doc """
  Returns a single unified `%SimpleGrpc.Schema{}` containing all Google Well-Known Types.
  """
  def unified_schema do
    @unified_schema
  end
end
