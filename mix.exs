defmodule JasminEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :jasmin_ex,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/project.plt"},
        plt_core_path: "priv/plts/core.plt"
      ],
      test_ignore_filters: [
        # `test/support/*` is test scaffolding (loaded via test_helper.exs),
        # not ExUnit test files — exclude it from the test_load_filters scan.
        ~r/test\/support\/.+\.ex$/
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {JasminEx.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:telemetry, "~> 1.0"}
    ]
  end
end
