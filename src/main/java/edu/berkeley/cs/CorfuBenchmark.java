package edu.berkeley.cs;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.serializer.Serializers;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Deprecated
abstract class CorfuBenchmark {
    private static String STREAM_NAME = "pkt";
    protected static byte[] DATA = new byte[54];

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
    }
    final static Logger LOG = Logger.getLogger(CorfuBenchmark.class.getName());

    private CorfuRuntime corfuRuntime;
    private List<SMRMap<Long, byte[]>> maps;
    private int batchSize;
    private int numIter;
    private int numThreads;

    CorfuBenchmark(String corfuConfigurationString, int batchSize, int numIter, int numThreads) {
        this.batchSize = batchSize;
        this.numIter = numIter;
        this.numThreads = numThreads;
        LOG.info("Creating new benchmark with: ");
        LOG.info("\thost/port: " + corfuConfigurationString);
        LOG.info("\tstream: " + STREAM_NAME);
        LOG.info("\tbatch-size: " + batchSize);
        LOG.info("\tnum-iterations: " + numIter);
        LOG.info("\tnum-threads: " + numThreads);
        this.maps = new ArrayList<>(numThreads);
        LOG.info("Initializing Corfu connections/structures...");
        setCorfuRuntime(getRuntimeAndConnect(corfuConfigurationString));
        for (int i = 0; i < numThreads; i++) {
            addMap(getCorfuRuntime().getObjectsView()
                    .build()
                    .setSerializer(Serializers.PRIMITIVE)
                    .setStreamName(STREAM_NAME + i)
                    .setType(SMRMap.class)
                    .open());
        }
        LOG.info("Corfu initialization complete.");
    }

    private CorfuRuntime getRuntimeAndConnect(String configurationString) {
        return new CorfuRuntime(configurationString).connect();
    }

    private CorfuRuntime getCorfuRuntime() {
        return corfuRuntime;
    }

    private void setCorfuRuntime(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
    }

    SMRMap<Long, byte[]> getMap(int i) {
        return maps.get(i);
    }

    private void addMap(SMRMap<Long, byte[]> map) {
        this.maps.add(map);
    }

    void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBuild().begin();
    }

    void TXEnd() {
        getCorfuRuntime().getObjectsView().TXEnd();
    }

    int getBatchSize() {
        return batchSize;
    }

    int getNumIter() {
        return numIter;
    }

    int getNumThreads() {
        return numThreads;
    }

    abstract void run();
}
