package edu.berkeley.cs;

import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Deprecated
public class LoadData extends CorfuBenchmark {
    LoadData(String conf, int batchSize, int numIter, int numThreads, String dataSource) {
        super(conf, batchSize, numIter, numThreads, dataSource);
    }

    class LoaderTask implements Callable<Result> {
        int tid;

        LoaderTask(int tid) {
            this.tid = tid;
        }

        public Result call() throws Exception {
            long numOps = 0;
            long numAborts = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumDataPts(); i += getBatchSize()) {
                try {
                    TXBegin();
                    for (int j = 0; j < getBatchSize(); j++) {
                        int dataIdx = i + j;
                        getMap(tid).blindPut(dataPoint(dataIdx).timestamp, dataPoint(dataIdx).value);
                    }
                    TXEnd();
                    numOps += getBatchSize();
                } catch (TransactionAbortedException ignored) {
                    numAborts++;
                }
                if (i > 0 && i % (getBatchSize() * 10) == 0)
                    LOG.info("Loaded " + i + " data points...");
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
        LOG.info("Loading data to " + getStreamName());
        ExecutorService executor = Executors.newFixedThreadPool(getNumThreads());
        List<LoaderTask> tasks = new ArrayList<>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new LoaderTask(i));
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
