package edu.berkeley.cs;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

@Deprecated
abstract class CorfuBenchmark {
    private static String STREAM_NAME = "test";
    private static int NUM_DATA_PTS = 100000000;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
    }
    final static Logger LOG = Logger.getLogger(CorfuBenchmark.class.getName());

    private CorfuRuntime corfuRuntime;
    private SMRMap<Long, Double> map;
    private int batchSize;
    private int numBatches;
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

    CorfuBenchmark(String corfuConfigurationString, int batchSize, int numBatches, int numThreads, String dataSource) {
        this.batchSize = batchSize;
        this.numBatches = numBatches;
        this.numThreads = numThreads;
        this.dataSource = dataSource;
        this.data = new DataPoint[NUM_DATA_PTS];
        readCSV();
        setCorfuRuntime(getRuntimeAndConnect(corfuConfigurationString));
        setMap(getCorfuRuntime().getObjectsView().build().setStreamName(STREAM_NAME).setType(SMRMap.class).open());
    }

    private CorfuRuntime getRuntimeAndConnect(String configurationString) {
        return new CorfuRuntime(configurationString).connect();
    }

    CorfuRuntime getCorfuRuntime() {
        return corfuRuntime;
    }

    private void setCorfuRuntime(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
    }

    SMRMap<Long, Double> getMap() {
        return map;
    }

    private void setMap(SMRMap<Long, Double> map) {
        this.map = map;
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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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

    public String getStreamName() {
        return STREAM_NAME;
    }

    int getBatchSize() {
        return batchSize;
    }

    int getNumBatches() {
        return numBatches;
    }

    int getNumThreads() {
        return numThreads;
    }

    DataPoint dataPoint(int i) {
        return data[i];
    }

    abstract void runBenchmark();
}
