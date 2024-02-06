defmodule Brc.Implementation.Charlie do
  @moduledoc """
  A variation on the Alpha solution where the worker processes write to a private ETS table.
  Also contains the following optimizations:
  - Manual temperature parsing instead of Float.parse
  - Temperature conversion to integers
  - Process binary chunk instead of opening IO device to chunk
  """

  require Logger

  @compile {:inline,
            [
              parse_temperature: 1,
              read_lines: 1
            ]}

  @behaviour Brc.Implementation
  @table_opts [
    :set,
    :named_table,
    {:write_concurrency, false},
    {:read_concurrency, false},
    {:decentralized_counters, true}
  ]

  def run(path) do
    num_bytes = File.stat!(path).size
    num_workers = Application.get_env(:brc, :processes, System.schedulers_online())
    bytes_per_worker = div(num_bytes, num_workers)
    worker_offset_args = calc_offset_args(num_bytes, num_workers, bytes_per_worker)

    timestamp_file_start = System.monotonic_time(:millisecond)

    {:ok, file_descriptor} =
      :prim_file.open(path, [:read, :raw, :binary, {:read_ahead, 2 * bytes_per_worker}])

    try do
      for {{offset_start, offset_end}, chunk_index} <- Enum.with_index(worker_offset_args) do
        num_bytes_to_read =
          if chunk_index == num_workers - 1 do
            offset_end - offset_start + 1
          else
            bytes_per_worker
          end

        {:ok, chunk} = :prim_file.read(file_descriptor, num_bytes_to_read)

        chunk =
          case :prim_file.read_line(file_descriptor) do
            :eof ->
              chunk

            {:ok, line} ->
              # patch incomplete lines
              <<chunk::binary, line::binary>>
          end

        :persistent_term.put("file_chunk_#{chunk_index}", chunk)
      end

      :ok
    after
      :prim_file.close(file_descriptor)
    end

    file_processing_seconds = (System.monotonic_time(:millisecond) - timestamp_file_start) / 1000

    Logger.info(
      "built #{num_workers} file chunks in #{Float.ceil(file_processing_seconds, 1)} sec"
    )

    timestamp_task_processing = System.monotonic_time(:millisecond)

    tasks =
      Enum.map(0..(num_workers - 1), fn worker_id ->
        process_chunk(worker_id)
      end)

    task_results = Task.await_many(tasks, :infinity)

    Logger.info(
      "all workers finished in #{Float.ceil((System.monotonic_time(:millisecond) - timestamp_task_processing) / 1000, 1)} sec"
    )

    timestamp_results = System.monotonic_time(:millisecond)

    results =
      Enum.reduce(task_results, %{}, fn {:ok, results}, acc ->
        Map.merge(acc, results, fn _k, {min1, max1, sum1, count1}, {min2, max2, sum2, count2} ->
          {min(min1, min2), max(max1, max2), sum1 + sum2, count1 + count2}
        end)
      end)
      |> Enum.map(fn {city, {min, max, sum, count}} ->
        {city, {min / 10, max / 10, sum / 10, count}}
      end)
      |> Map.new()

    Logger.info(
      "results fetched in #{Float.ceil((System.monotonic_time(:millisecond) - timestamp_results) / 1000, 1)} sec"
    )

    dbg(:erlang.memory())

    {:ok, results}
  end

  defp process_chunk(index) do
    Task.async(fn ->
      "file_chunk_#{index}"
      |> :persistent_term.get()
      |> then(fn chunk ->
        Process.put(:table, :ets.new(String.to_atom("results_shard_#{index}"), @table_opts))
        read_lines(chunk)
      end)
    end)
  end

  defp read_lines(<<>>) do
    table = Process.get(:table)

    results =
      table
      |> :ets.tab2list()
      |> Enum.map(fn {city, min, max, sum, count} -> {city, {min, max, sum, count}} end)
      |> Map.new()

    :ets.delete(table)
    {:ok, results}
  end

  defp read_lines(binary) do
    [line, rest] = :binary.split(binary, "\n")
    [city, temp_string] = :binary.split(line, ";")
    # fixed point aritmetics: will read string as an integer in order to
    # allow counter operations in ETS. Since it has 1 decimal place we just
    # multiply the end result by 10 when reading. This is undone when
    # collecting results.
    temp = parse_temperature(temp_string)

    table = Process.get(:table)

    case :ets.lookup(table, city) do
      [] ->
        :ets.insert(table, {city, temp, temp, temp, 1})

      [{city, _min, max, _sum, _count}] ->
        delta_max =
          if max >= temp do
            0
          else
            temp - max
          end

        :ets.update_counter(
          table,
          city,
          [
            # if min + 0 > temp, set to temp
            {2, 0, temp, temp},
            # no way to express update_op for max,
            # default to incrementing by delta_max
            {3, delta_max},
            # sum
            {4, temp},
            # count
            {5, 1}
          ],
          {city, temp, temp, temp, 1}
        )
    end

    read_lines(rest)
  end

  @doc """
  Parses a temperature in the range of [-99, 99] with exactly 1 decimal place.
  (e.g. -12.3, 45.9, 0.0)
  """
  def parse_temperature(<<t1, t2, ".", d1, _rest::binary>>)
      when t1 in ?0..?9 and t2 in ?0..?9 and d1 in ?0..?9 do
    (t1 - ?0) * 100 + (t2 - ?0) * 10 + (d1 - ?0)
  end

  def parse_temperature(<<"-", t1, t2, ".", d1, _rest::binary>>)
      when t1 in ?0..?9 and t2 in ?0..?9 and d1 in ?0..?9 do
    -((t1 - ?0) * 100 + (t2 - ?0) * 10 + (d1 - ?0))
  end

  def parse_temperature(<<t1, ".", d1, _rest::binary>>)
      when t1 in ?0..?9 and d1 in ?0..?9 do
    (t1 - ?0) * 10 + (d1 - ?0)
  end

  def parse_temperature(<<"-", t1, ".", d1, _rest::binary>>)
      when t1 in ?0..?9 and d1 in ?0..?9 do
    -((t1 - ?0) * 10 + (d1 - ?0))
  end

  # defp parse_temperature(temp), do: raise "temperature #{temp} does not have 1-2 digits + exactly 1 decimal place"

  # defp parse_temperature(<<"-", rest::binary>>, _acc) do
  #   parse_temperature(rest, -1)
  # end

  # defp parse_temperature(<<".", rest::binary>>, acc) do
  #   parse_temperature(rest, acc)
  # end

  # defp parse_temperature(<<character, rest::binary>>, acc)
  #      when character >= ?0 and character <= ?9 do
  #   parse_temperature(rest, acc * 10 + (character - ?0))
  # end

  # defp parse_temperature(<<"\n", _rest::binary>>, acc), do: acc

  defp calc_offset_args(_num_bytes, num_workers, bytes_per_worker) do
    calc_offset_args_rec([], 0, num_workers, bytes_per_worker)
  end

  defp calc_offset_args_rec(acc, _offset_start, 0, _bytes_per_worker) do
    Enum.reverse(acc)
  end

  defp calc_offset_args_rec(acc, offset_start, num_workers, bytes_per_worker) do
    offset_end = offset_start + bytes_per_worker - 1

    calc_offset_args_rec(
      [{offset_start, offset_end} | acc],
      offset_end,
      num_workers - 1,
      bytes_per_worker
    )
  end
end
