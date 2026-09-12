defmodule SimpleGrpc.RegistryTest do
  use ExUnit.Case, async: false

  alias SimpleGrpc.{Registry, Dispatcher, Schema}

  setup do
    Registry.clear()
    :ok
  end

  describe "Service & Route Registration" do
    test "registers a service schema with lambda handlers" do
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")

      {:ok, paths} =
        Registry.register_service(schema, %{
          echo: fn req -> %{reply: "Echoed: " <> req.message} end
        })

      assert "/custom_prefix.PrefixService/Echo" in paths

      {:ok, route} = Registry.lookup_route("/custom_prefix.PrefixService/Echo")
      assert route.service_name == "PrefixService"
      assert route.rpc_name == "Echo"
      assert route.input_message.name == "EchoRequest"
      assert route.output_message.name == "EchoResponse"

      # Dispatch test
      {:ok, resp} = Dispatcher.dispatch(route, %{message: "Dynamic gRPC"})
      assert resp.reply == "Echoed: Dynamic gRPC"
    end

    test "hot-replaces an RPC handler at runtime" do
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")

      Registry.register_service(schema, %{
        echo: fn %{message: m} -> %{reply: "v1: " <> m} end
      })

      path = "/custom_prefix.PrefixService/Echo"
      {:ok, route_v1} = Registry.lookup_route(path)
      {:ok, resp_v1} = Dispatcher.dispatch(route_v1, %{message: "test"})
      assert resp_v1.reply == "v1: test"

      # Replace handler dynamically
      Registry.register_rpc(path, %{
        route_v1
        | handler: fn %{message: m} -> %{reply: "v2: " <> m} end
      })

      {:ok, route_v2} = Registry.lookup_route(path)
      {:ok, resp_v2} = Dispatcher.dispatch(route_v2, %{message: "test"})
      assert resp_v2.reply == "v2: test"
    end

    test "unregisters service routes" do
      {:ok, schema} = Schema.from_file("priv/protos/custom_prefix/custom_prefix_service.proto")
      Registry.register_service(schema, %{echo: fn req -> req end})

      assert length(Registry.list_routes()) == 1
      assert "custom_prefix.PrefixService" in Registry.list_services()

      Registry.unregister_service("custom_prefix.PrefixService")
      assert Registry.list_routes() == []
      assert Registry.lookup_route("/custom_prefix.PrefixService/Echo") == :not_found
    end
  end

  describe "Dispatcher Error Handling" do
    test "formats custom error tuples" do
      route = %{
        handler: fn _req -> {:error, 5, "Not found in database"} end,
        rpc_name: "Echo"
      }

      assert {:error, 5, "Not found in database"} = Dispatcher.dispatch(route, %{})
    end

    test "raises exceptions — caught at the Plug boundary, not the Dispatcher" do
      route = %{
        handler: fn _req -> raise "Something went wrong" end,
        rpc_name: "Echo"
      }

      assert_raise RuntimeError, "Something went wrong", fn ->
        Dispatcher.dispatch(route, %{})
      end
    end
  end
end
