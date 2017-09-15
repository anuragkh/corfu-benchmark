package edu.berkeley.cs;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

@Deprecated
class AggregateBenchmark extends CorfuBenchmark {

    AggregateBenchmark(String conf, int batchSize, int numIter, int numThreads, String dataSource) {
        super(conf, batchSize, numIter, numThreads, dataSource);
    }

    class AggregateTask implements Callable<Result> {
        private int tid;

        AggregateTask(int tid) {
            this.tid = tid;
        }

        public Result call() throws Exception {
            long numOps = 0;
            int count = 0;
            double sum = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                int dataIdx = (new Random()).nextInt(getNumDataPts() - getBatchSize());
                Long t1 = dataPoint(dataIdx).timestamp;
                Long t2 = dataPoint(dataIdx + getBatchSize()).timestamp;
                Predicate<Map.Entry<Long, Double>> query = p -> (p.getKey() >= t1) && (p.getKey() <= t2);
                Collection<Map.Entry<Long, Double>> res = getMap(tid).scanAndFilterByEntry(query);
                count = res.size();
                sum = 0;
                min = Double.MAX_VALUE;
                max = -Double.MAX_VALUE;
                for (Map.Entry<Long, Double> entry : res) {
                    sum += entry.getValue();
                    min = Double.min(min, entry.getValue());
                    max = Double.max(max, entry.getValue());
                }
                numOps += 1;
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            double thput = (double) (numOps) / (totTime / 1000.0);
            double latency = (double) totTime / (double) (numOps);
            LOG.info("sum = " + sum + " min = " + min + " max = " + max + " count = " + count);
            return new Result(thput, latency);
        }
    }

    void run() {
        LOG.info("Running aggregate benchmark...");
        ExecutorService executor = Executors.newFixedThreadPool(getNumThreads());
        List<AggregateTask> tasks = new ArrayList<>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new AggregateTask(i));
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
