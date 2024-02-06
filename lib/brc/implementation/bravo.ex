defmodule Brc.Implementation.Bravo do
  @moduledoc """
  A variation on the Alpha solution using custom line parsing.
  """

  require Logger

  @behaviour Brc.Implementation

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
      |> Enum.map(fn {k, {min, max, sum, count}} -> {k, {min / 10, max / 10, sum / 10, count}} end)
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
      |> StringIO.open()
      |> then(fn {:ok, device} ->
        Process.put(:results, %{})
        read_lines(device)
      end)
    end)
  end

  defp read_lines(device) do
    case IO.read(device, :line) do
      :eof ->
        {:ok, Process.get(:results)}

      {:error, reason} ->
        throw(reason)

      line ->
        city_length_bytes = count_bytes_before_delimitor(line)
        <<city::binary-size(city_length_bytes), ";", temp_string::binary>> = line
        temp = parse_temperature(temp_string)

        results =
          Map.update(Process.get(:results), city, {temp, temp, temp, 1}, fn {min, max, sum, count} ->
            {min(min, temp), max(max, temp), temp + sum, count + 1}
          end)

        Process.put(:results, results)

        read_lines(device)
    end
  end

  defp count_bytes_before_delimitor(binary, count \\ 0)

  defp count_bytes_before_delimitor(<<";", _rest::binary>>, count), do: count

  defp count_bytes_before_delimitor(<<_character, rest::binary>>, count) do
    count_bytes_before_delimitor(rest, count + 1)
  end

  defp parse_temperature(binary, acc \\ 1)

  defp parse_temperature(<<"-", rest::binary>>, _acc) do
    parse_temperature(rest, -1)
  end

  defp parse_temperature(<<".", rest::binary>>, acc) do
    parse_temperature(rest, acc)
  end

  defp parse_temperature(<<character, rest::binary>>, acc)
       when character >= ?0 and character <= ?9 do
    parse_temperature(rest, acc * 10 + (character - ?0))
  end

  defp parse_temperature(<<"\n", _rest::binary>>, acc), do: acc

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
