package edu.berkeley.cs;

@Deprecated
public class LoadData extends CorfuBenchmark {
    LoadData(String conf, int batchSize, int numBatches, int numThreads, String dataSource) {
        super(conf, batchSize, numBatches, numThreads, dataSource);
    }

    void runBenchmark() {
        LOG.info("Loading data to " + getStreamName());
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < getNumDataPts(); i += getBatchSize()) {
            TXBegin();
            for (int j = 0; j < getBatchSize(); j++) {
                DataPoint p = dataPoint(i + j);
                getMap().put(p.timestamp, p.value);
            }
            TXEnd();
            if (i != 0 && i % 10000 == 0) {
                LOG.info("Loaded " + i + " data points...");
            }
        }
        long endTime = System.currentTimeMillis();
        long totTime = endTime - startTime;
        LOG.info("Load took " + totTime + " ms");
    }
}
