package edu.berkeley.cs;

class ReadBenchmark extends CorfuBenchmark {

    ReadBenchmark(String conf, String streamName) {
        super(conf, streamName);
    }

    void runBenchmark(int batchSize, int numBatches) {
        String batch[] = new String[batchSize];
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numBatches; i++) {
            getCorfuRuntime().getObjectsView().TXBegin();
            for (int j = 0; j < batchSize; j++) {
                long key = (i * batchSize + j);
                batch[j] = getMap().get(key);
            }
            getCorfuRuntime().getObjectsView().TXEnd();
        }
        long endTime = System.currentTimeMillis();
        long totTime = endTime - startTime;
        double throughput = (double) (batchSize * numBatches) / (totTime / 1000.0);
        double latency = (double) totTime / (double) (batchSize * numBatches);
        System.out.println(throughput + " " + latency);
    }
}
