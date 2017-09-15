package edu.berkeley.cs;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.serializer.Serializers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Deprecated
abstract class CorfuBenchmark {
    private static String STREAM_NAME = "test";
    private static int NUM_DATA_PTS = 16777216;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
    }
    final static Logger LOG = Logger.getLogger(CorfuBenchmark.class.getName());

    private CorfuRuntime corfuRuntime;
    private List<SMRMap<Long, Double>> maps;
    private int batchSize;
    private int numIter;
    private int numThreads;
    private String dataSource;

    class DataPoint {
        long timestamp;
        double value;

        DataPoint(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }
    private DataPoint[] data;

    CorfuBenchmark(String corfuConfigurationString, int batchSize, int numIter, int numThreads, String dataSource) {
        this.batchSize = batchSize;
        this.numIter = numIter;
        this.numThreads = numThreads;
        this.dataSource = dataSource;
        LOG.info("Creating new benchmark with: ");
        LOG.info("\thost/port: " + corfuConfigurationString);
        LOG.info("\tstream: " + STREAM_NAME);
        LOG.info("\tbatch-size: " + batchSize);
        LOG.info("\tnum-iterations: " + numIter);
        LOG.info("\tnum-threads: " + numThreads);
        LOG.info("\tdata-source: " + dataSource);
        this.maps = new ArrayList<>(numThreads);
        this.data = new DataPoint[NUM_DATA_PTS];
        readCSV();
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

    SMRMap<Long, Double> getMap(int i) {
        return maps.get(i);
    }

    private void addMap(SMRMap<Long, Double> map) {
        this.maps.add(map);
    }

    void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBuild().begin();
    }

    void TXEnd() {
        getCorfuRuntime().getObjectsView().TXEnd();
    }

    private void readCSV() {
        LOG.info("Reading data from " + dataSource + "...");
        BufferedReader br = null;
        String line;
        String cvsSplitBy = ",";
        int i = 0;
        try {
            br = new BufferedReader(new FileReader(dataSource));
            while ((line = br.readLine()) != null && i < data.length) {
                // use comma as separator
                String[] csv = line.split(cvsSplitBy);
                if (csv.length != 2) {
                    System.out.println("Could not parse CSV line " + i + ": " + line);
                    System.exit(1);
                }
                data[i] = new DataPoint(Long.parseLong(csv[0]), Double.parseDouble(csv[1]));
                ++i;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("Finished reading data: " + i + " data points.");
    }

    int getNumDataPts() {
        return NUM_DATA_PTS;
    }

    String getStreamName() {
        return STREAM_NAME;
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

    DataPoint dataPoint(int i) {
        return data[i];
    }

    abstract void run();
}
