package edu.berkeley.cs;

import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Deprecated
class WriteBenchmark extends CorfuBenchmark {

    WriteBenchmark(String conf, int batchSize, int numIter, int numThreads, String dataSource) {
        super(conf, batchSize, numIter, numThreads, dataSource);
    }

    class WriterTask implements Callable<Result> {
        public Result call() throws Exception {
            long numOps = 0;
            long numAborts = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                try {
                    TXBegin();
                    for (int j = 0; j < getBatchSize(); j++) {
                        int dataIdx = i * getBatchSize() + j;
                        getMap().put(dataPoint(dataIdx).timestamp, dataPoint(dataIdx).value);
                    }
                    TXEnd();
                    numOps += getBatchSize();
                } catch (TransactionAbortedException ignored) {
                    numAborts++;
                }
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            double thput = (double) numOps / (totTime / 1000.0);
            double latency = (double) totTime / (double) numOps;
            if (numAborts > 0)
                LOG.warning(numAborts + " tx aborts");
            return new Result(thput, latency);
        }
    }

    void run() {
        LOG.info("Running write benchmark...");
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
