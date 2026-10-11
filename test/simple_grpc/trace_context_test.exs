defmodule SimpleGrpc.TraceContextTest do
  use ExUnit.Case, async: true

  alias SimpleGrpc.TraceContext
  alias SimpleGrpc.Context

  describe "generate and format" do
    test "generates standard 00 version W3C traceparent" do
      tc = TraceContext.generate()
      assert tc.version == "00"
      assert String.length(tc.trace_id) == 32
      assert String.length(tc.span_id) == 16
      assert tc.flags == "01"
      assert tc.sampled? == true

      formatted = TraceContext.format(tc)
      assert formatted =~ ~r/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/
    end

    test "generates child span retaining trace_id" do
      parent = TraceContext.generate()
      child = TraceContext.child_span(parent)

      assert child.trace_id == parent.trace_id
      assert child.parent_id == parent.span_id
      assert child.span_id != parent.span_id
      assert child.flags == parent.flags
    end
  end

  describe "parse" do
    test "parses valid traceparent header" do
      header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
      assert {:ok, tc} = TraceContext.parse(header)
      assert tc.version == "00"
      assert tc.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
      assert tc.span_id == "00f067aa0ba902b7"
      assert tc.flags == "01"
      assert tc.sampled? == true
    end

    test "rejects all-zero trace_id or span_id" do
      assert TraceContext.parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01") ==
               :error

      assert TraceContext.parse("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01") ==
               :error
    end

    test "rejects invalid formats" do
      assert TraceContext.parse("invalid-header") == :error
      assert TraceContext.parse(nil) == :error
      assert TraceContext.parse("") == :error
    end
  end

  describe "extract and inject" do
    test "injects and extracts from Context" do
      tc = TraceContext.generate(tracestate: "rojo=123")
      ctx = %Context{}
      ctx = TraceContext.inject(ctx, tc)

      assert Context.get_header(ctx, "traceparent") == TraceContext.format(tc)
      assert Context.get_header(ctx, "tracestate") == "rojo=123"

      assert {:ok, extracted} = TraceContext.extract(ctx)
      assert extracted.trace_id == tc.trace_id
      assert extracted.span_id == tc.span_id
      assert extracted.tracestate == "rojo=123"
    end

    test "injects and extracts from header list" do
      tc = TraceContext.generate()
      headers = [{"content-type", "application/grpc"}]
      injected = TraceContext.inject(headers, tc)

      assert {:ok, extracted} = TraceContext.extract(injected)
      assert extracted.trace_id == tc.trace_id
    end
  end
end
