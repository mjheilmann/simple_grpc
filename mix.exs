defmodule SimpleGrpc.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mjheilmann/simple_grpc"

  def project do
    [
      app: :simple_grpc,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      description:
        "Zero-boilerplate, pure-Elixir gRPC and Protobuf implementation with compile-time macros, HTTP/2 pooling, and gRPC-Web.",
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :telemetry]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib priv mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: ["README.md", "docs/concepts.md", "LICENSE"]
    ]
  end

  defp deps do
    [
      {:nimble_parsec, "~> 1.2"},
      {:bandit, "~> 1.0"},
      {:plug, "~> 1.16"},
      {:mint, "~> 1.6"},
      {:nimble_pool, "~> 1.0"},
      {:castore, "~> 1.0", optional: true},
      {:telemetry, "~> 1.0 or ~> 1.2"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end
end
