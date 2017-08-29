package edu.berkeley.cs;

@Deprecated
public class LoadData extends CorfuBenchmark {
    public LoadData(String conf, int batchSize, int numBatches, int numThreads, String dataSource) {
        super(conf, batchSize, numBatches, numThreads, dataSource);
    }

    void runBenchmark() {
        LOG.info("Loading data to " + getStreamName());
        long startTime = System.currentTimeMillis();
        TXBegin();
        for (int i = 0; i < getNumDataPts(); i++) {
            getMap().put(dataPoint(i).timestamp, dataPoint(i).value);
        }
        TXEnd();
        long endTime = System.currentTimeMillis();
        long totTime = endTime - startTime;
        LOG.info("Load took " + totTime + " ms");
    }
}
