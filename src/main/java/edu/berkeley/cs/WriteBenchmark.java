package edu.berkeley.cs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Deprecated
class WriteBenchmark extends CorfuBenchmark {

    WriteBenchmark(String conf, int batchSize, int numBatches, int numThreads, String dataSource) {
        super(conf, batchSize, numBatches, numThreads, dataSource);
    }

    class WriterTask implements Callable<Result> {
        public Result call() throws Exception {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumBatches(); i++) {
                TXBegin();
                for (int j = 0; j < getBatchSize(); j++) {
                    int dataIdx = i * getBatchSize() + j;
                    getMap().put(dataPoint(dataIdx).timestamp, dataPoint(dataIdx).value);
                }
                TXEnd();
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            double thput = (double) (getNumBatches() * getBatchSize()) / (totTime / 1000.0);
            double latency = (double) (totTime) / (double) (getNumBatches() * getBatchSize());
            return new Result(thput, latency);
        }
    }

    void runBenchmark() {
        LOG.info("Running Write Benchmark");
        ExecutorService executor = Executors.newFixedThreadPool(getNumThreads());
        List<WriterTask> tasks = new ArrayList<>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new WriterTask());
        }
        List<Future<Result>> futures = null;
        try {
            futures = executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        double throughput = 0;
        double latency = 0;
        for (Future<Result> future : futures) {
            try {
                Result result = future.get();
                throughput += result.throughput;
                latency += result.latency;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

        }
        latency /= getNumThreads();
        System.out.println(throughput + " " + latency);
        executor.shutdown();
    }
}
