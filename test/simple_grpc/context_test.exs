defmodule SimpleGrpc.ContextTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.Context

  describe "header manipulation" do
    test "put_header and get_header are case-insensitive and normalize keys" do
      ctx = %Context{}
      ctx = Context.put_header(ctx, "Authorization", "Bearer token123")
      ctx = Context.put_header(ctx, :x_custom_id, "custom-456")

      assert Context.get_header(ctx, "authorization") == "Bearer token123"
      assert Context.get_header(ctx, "AUTHORIZATION") == "Bearer token123"
      assert Context.get_header(ctx, :authorization) == "Bearer token123"

      assert Context.get_header(ctx, "x-custom-id") == "custom-456"
      assert Context.get_header(ctx, :x_custom_id) == "custom-456"

      assert ctx.req_headers == ctx.headers
    end

    test "put_header replaces existing header with same name" do
      ctx =
        %Context{}
        |> Context.put_header("x-trace-id", "trace-1")
        |> Context.put_header("X-Trace-Id", "trace-2")

      assert Context.get_header(ctx, "x-trace-id") == "trace-2"
      assert length(ctx.headers) == 1
    end

    test "delete_header removes header" do
      ctx =
        %Context{}
        |> Context.put_header("x-to-delete", "val")
        |> Context.delete_header("X-To-Delete")

      assert Context.get_header(ctx, "x-to-delete") == nil
      assert ctx.headers == []
    end
  end

  describe "private and metadata storage" do
    test "put_private, get_private, and update_private" do
      ctx =
        %Context{}
        |> Context.put_private(:user_id, 42)
        |> Context.update_private(:user_id, 0, &(&1 + 10))

      assert Context.get_private(ctx, :user_id) == 52
      assert Context.get_private(ctx, :non_existent, "default") == "default"
    end

    test "put_metadata and get_metadata with map" do
      ctx =
        %Context{metadata: %{}}
        |> Context.put_metadata(:session, "abc")

      assert Context.get_metadata(ctx, :session) == "abc"
      assert Context.get_metadata(ctx, :other, :none) == :none
    end
  end

  describe "deadline and timeout helpers with Context" do
    test "extracts deadline and timeout from Context headers" do
      ctx =
        %Context{}
        |> Context.put_header("grpc-timeout", "3000m")

      deadline = SimpleGrpc.deadline(ctx)
      assert %DateTime{} = deadline
      assert DateTime.diff(deadline, DateTime.utc_now(), :millisecond) in 2500..3500

      assert SimpleGrpc.remaining_timeout(ctx) == 3000
      assert SimpleGrpc.remaining_timeout(ctx, :second) == 3
      assert SimpleGrpc.remaining_timeout(ctx, :microsecond) == 3_000_000
    end
  end
end
