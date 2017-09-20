package edu.berkeley.cs;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Deprecated
class ReadBenchmark extends CorfuBenchmark {

    ReadBenchmark(String conf, int batchSize, int numIter, int numThreads, String dataSource) {
        super(conf, batchSize, numIter, numThreads, dataSource);
    }

    class ReaderTask implements Callable<Result> {
        private int tid;
        ReaderTask(int tid) {
            this.tid = tid;
        }

        public Result call() throws Exception {
            // Warmup
            for (int i = 0; i < getNumIter() / 10; i++) {
                Long timestamp = dataPoint((new Random()).nextInt(getNumDataPts())).timestamp;
                long t1 = System.currentTimeMillis();
                getMap(tid).get(timestamp);
                long t2 = System.currentTimeMillis();
                long totTime = t2 - t1;
                LOG.info("Read " + i + " took " + totTime + "ms");
            }

            long numOps = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                Long timestamp = dataPoint((new Random()).nextInt(getNumDataPts())).timestamp;
                getMap(tid).get(timestamp);
                numOps += 1;
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            double thput = (double) (numOps) / (totTime / 1000.0);
            double latency = (double) totTime / (double) (numOps);
            return new Result(thput, latency);
        }
    }

    void run() {
        LOG.info("Running read benchmark...");
        ExecutorService executor = Executors.newFixedThreadPool(getNumThreads());
        List<ReaderTask> tasks = new ArrayList<>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new ReaderTask(0));
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
