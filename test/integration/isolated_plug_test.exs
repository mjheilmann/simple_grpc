defmodule SimpleGrpc.IsolatedPlugTest do
  use ExUnit.Case, async: false

  @port_a 50091
  @port_b 50092

  defmodule ServiceA do
    use SimpleGrpc.Server,
      server_file: "priv/protos/streaming/streaming_service.proto"

    def unary_call(%{message: msg, sequence: seq}, _stream) do
      {:ok, %{result: "ServiceA: " <> msg, sequence: seq + 10, is_final: true}}
    end

    def server_streaming_call(r, _), do: r
    def client_streaming_call(r, _), do: r
    def bidirectional_streaming_call(r, _), do: r
  end

  defmodule ServiceB do
    use SimpleGrpc.Server,
      server_file: "priv/protos/global_service.proto"

    def global_method(%{data: d}, _stream) do
      {:ok, %{result: "ServiceB: " <> d}}
    end
  end

  setup_all do
    grpcurl = System.find_executable("grpcurl") || "/opt/homebrew/bin/grpcurl"

    {:ok, pid_a} =
      Bandit.start_link(
        plug: {SimpleGrpc.Plug, services: [ServiceA], reflection: true},
        port: @port_a
      )

    {:ok, pid_b} =
      Bandit.start_link(
        plug: {SimpleGrpc.Plug, services: [ServiceB], reflection: true},
        port: @port_b
      )

    on_exit(fn ->
      if Process.alive?(pid_a), do: Process.exit(pid_a, :shutdown)
      if Process.alive?(pid_b), do: Process.exit(pid_b, :shutdown)
    end)

    %{grpcurl: grpcurl}
  end

  test "port A only hosts and reflects ServiceA (isolated from ServiceB)", %{grpcurl: grpcurl} do
    {output_a, 0} =
      System.cmd(grpcurl, ["-plaintext", "127.0.0.1:#{@port_a}", "list"], stderr_to_stdout: true)

    assert output_a =~ "streaming.StreamingService"
    refute output_a =~ "GlobalService"

    {call_out, 0} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-d",
          ~s({"message": "Hello A", "sequence": 1}),
          "127.0.0.1:#{@port_a}",
          "streaming.StreamingService/UnaryCall"
        ],
        stderr_to_stdout: true
      )

    assert call_out =~ "ServiceA: Hello A"
  end

  test "port B only hosts and reflects ServiceB (isolated from ServiceA)", %{grpcurl: grpcurl} do
    {output_b, 0} =
      System.cmd(grpcurl, ["-plaintext", "127.0.0.1:#{@port_b}", "list"], stderr_to_stdout: true)

    assert output_b =~ "GlobalService"
    refute output_b =~ "streaming.StreamingService"

    {call_out, 0} =
      System.cmd(
        grpcurl,
        [
          "-plaintext",
          "-d",
          ~s({"data": "Hello B"}),
          "127.0.0.1:#{@port_b}",
          "GlobalService/GlobalMethod"
        ],
        stderr_to_stdout: true
      )

    assert call_out =~ "ServiceB: Hello B"
  end
end
