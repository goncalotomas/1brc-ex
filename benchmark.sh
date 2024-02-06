mkdir -p benchmarks
mix escript.build > /dev/null

for rows in "1m" "10m"; do
    for processes in 1 2 4 8 16 32; do
        for run in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
            ./brc -p $processes ../../1brc/measurements_${rows}.txt > benchmarks/benchmark_${rows}_${processes}procs_run_${run}.txt
        done
    done


    # for processes in 8 16 32 64 128 256 1024
    # do
    #     ./brc -p $processes ../../1brc/measurements_100m.txt > benchmarks/benchmark_100m_${proc}processes_run_${x}.txt
    #     ./brc -p $processes ../../1brc/measurements_1b.txt > benchmarks/benchmark_1b_${proc}processes_run_${x}.txt
    # done
done

# results_1m = grep

# grep returns results in the format
# benchmarks/benchmark_1m_8procs_run_9.txt:Execution time: 0.44 seconds

# grep "Execution time: " benchmarks/benchmark_1m* | while read -r line; do echo $line | awk '{print $3}' > benchmarks; done

# compile results

for rows in "1m" "10m"
do
    for processes in 1 2 4 8 16 32
    do
        # aggregate data on all runs
        aggregate_file="benchmarks/benchmarks_${rows}_${processes}procs_results.txt"
        for file in $(ls benchmarks/benchmark_${rows}_${processes}procs*)
        do
            grep "Execution time: " $file | awk '{print $3}' >> $aggregate_file
        done
        sort -n $aggregate_file -o $aggregate_file
        echo "stats for benchmark_${rows}_${processes}procs"
        cat $aggregate_file |  python3 -c "import fileinput as FI,statistics as STAT; i = [float(l.strip()) for l in FI.input()]; print('min:', min(i), ' max: ', max(i), ' avg: ', STAT.mean(i), ' median: ', STAT.median(i))"
        
    done
done

rm -r benchmarks/
