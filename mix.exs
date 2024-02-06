defmodule Brc.MixProject do
  use Mix.Project

  def project do
    [
      app: :brc,
      escript: [
        main_module: Brc,
        emu_args: "+SDio 16"
      ],
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:eflambe, "~> 0.3.0", only: :dev}
    ]
  end
end
