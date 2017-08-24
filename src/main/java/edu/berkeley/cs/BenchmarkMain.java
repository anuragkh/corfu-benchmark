package edu.berkeley.cs;

import org.apache.commons.cli.*;

public class BenchmarkMain {
    public static void main(String[] args) {
        Options options = new Options();

        Option benchTypeOpt = new Option("t", "type", true, "Benchmark type (read/write)");
        benchTypeOpt.setType(String.class);
        options.addOption(benchTypeOpt);

        Option benchStreamOpt = new Option("s", "stream", true, "Corfu stream to read from/write to");
        benchStreamOpt.setType(String.class);
        options.addOption(benchStreamOpt);

        Option confOpt = new Option("c", "conf", true, "Corfu configuration string");
        confOpt.setType(String.class);
        options.addOption(confOpt);

        Option dataSizeOpt = new Option("d", "data-size", true, "Read/write data size");
        dataSizeOpt.setType(Integer.class);
        options.addOption(dataSizeOpt);

        Option batchSizeOpt = new Option("b", "batch-size", true, "Read/write batch size");
        batchSizeOpt.setType(Integer.class);
        options.addOption(batchSizeOpt);

        Option numBatchesOpt = new Option("n", "num-batches", true, "Number of batches to read/write");
        numBatchesOpt.setType(Integer.class);
        options.addOption(numBatchesOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("corfu-bench", options);
            System.exit(1);
        }

        String benchType = cmd.getOptionValue("type", "write");
        String conf = cmd.getOptionValue("conf", "localhost:9000");
        String streamName = cmd.getOptionValue("stream", "test");
        int dataSize = Integer.parseInt(cmd.getOptionValue("data-size", "64"));
        int batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", "1"));
        int numBatches = Integer.parseInt(cmd.getOptionValue("num-batches", "1000"));

        if (benchType.equalsIgnoreCase("write")) {
            WriteBenchmark bench = new WriteBenchmark(conf, streamName);
            bench.runBenchmark(dataSize, batchSize, numBatches);
        } else if (benchType.equalsIgnoreCase("read")) {
            ReadBenchmark bench = new ReadBenchmark(conf, streamName);
            bench.runBenchmark(batchSize, numBatches);
        }
    }
}
