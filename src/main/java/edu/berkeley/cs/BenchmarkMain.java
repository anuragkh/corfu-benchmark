package edu.berkeley.cs;

import org.apache.commons.cli.*;

@Deprecated
public class BenchmarkMain {
    public static void main(String[] args) {
        Options options = new Options();

        Option confOpt = new Option("c", "conf", true, "Corfu configuration string");
        confOpt.setType(String.class);
        options.addOption(confOpt);

        Option batchSizeOpt = new Option("b", "batch-size", true, "Read/write batch size");
        batchSizeOpt.setType(Integer.class);
        options.addOption(batchSizeOpt);

        Option numBatchesOpt = new Option("n", "num-batches", true, "Number of batches to read/write");
        numBatchesOpt.setType(Integer.class);
        options.addOption(numBatchesOpt);

        Option numThreadsOpt = new Option("t", "num-threads", true, "Number of benchmark threads");
        numThreadsOpt.setType(Integer.class);
        options.addOption(numThreadsOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("bench", options);
            System.exit(1);
        }

        String conf = cmd.getOptionValue("conf", "localhost:9000");
        int batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", "1"));
        int numBatches = Integer.parseInt(cmd.getOptionValue("num-batches", "1000"));
        int numThreads = Integer.parseInt(cmd.getOptionValue("num-threads", "1"));

        CorfuBenchmark bench = new WriteBenchmark(conf, batchSize, numBatches, numThreads);
        bench.run();
    }
}
