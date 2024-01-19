defmodule Brc.Implementation do
  @moduledoc """
  The behaviour for an implementation of the 1 billion row problem.
  Using a behaviour module allows for multiple different implementations
  that only need to solve the main logic,
  """

  @doc """
  The main callback for the implementation.
  Takes in the path to the input file and expects the results as a map with the
  city names as keys and a tuple `{min, max, sum, count}` as values, where:
  - `min` is the minimum recorded temperature
  - `max` is the maximum recorded temperature
  - `sum` is the sum of all recorded temperatures
  - `count` is the number of recorded temperatures

  The module which calls the run/1 callback will print the results according to format
  specified in the challenge.
  """
  @callback run(path :: String.t()) :: {:ok, results :: map()} | {:error, reason :: term()}
end
