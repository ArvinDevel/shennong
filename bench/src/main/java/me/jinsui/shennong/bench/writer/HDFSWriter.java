package me.jinsui.shennong.bench.writer;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.avro.User;
import me.jinsui.shennong.bench.source.AvroDataSource;
import me.jinsui.shennong.bench.source.DataSource;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


/**
 * Write avro data to hdfs as parquet format.
 */
@Slf4j
public class HDFSWriter extends Writer {

    /**
     * Flags for the write command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-u", "--url"
            },
            description = "HDFS cluster namenode url")
        public String url = "hdfs://localhost:9000";

        @Parameter(
            names = {
                "-r", "--rate"
            },
            description = "Write rate bytes/s across all file")
        public double writeRate = 1000000;

        @Parameter(
            names = {
                "-sf", "--schema-file"
            },
            description = "Schema represented as Avro, used in complex mode")
        public String schemaFile = null;

        @Parameter(
            names = {
                "-bs", "--block-size"
            },
            description = "Block size of parquet")
        public int blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

        @Parameter(
            names = {
                "-ps", "--page-size"
            },
            description = "Page size of parquet")
        public int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

        @Parameter(
            names = {
                "-d", "--directory"
            },
            description = "Directory name")
        public String directory = "/data/parquets/";

        @Parameter(
            names = {
                "-fn", "--file-name"
            },
            description = "File name")
        public String fileName = "test-file-%06d";

        @Parameter(
            names = {
                "-rs", "--replica-size"
            },
            description = "Number of replica of the file")
        public short replicaSize = 3;

        @Parameter(
            names = {
                "-fnum", "--file-num"
            },
            description = "File num")
        public int numFiles = 1;

        @Parameter(
            names = {
                "-t", "--threads"
            },
            description = "Number of threads writing")
        public int numThreads = 1;

        @Parameter(
            names = {
                "-n", "--num-events"
            },
            description = "Number of events to write in total. If 0, it will keep writing")
        public long numEvents = 0;

        @Parameter(
            names = {
                "-b", "--num-bytes"
            },
            description = "Number of bytes to write in total. If 0, it will keep writing")
        public long numBytes = 0;

    }

    private final Flags flags;
    private final DataSource<GenericRecord> dataSource;

    public HDFSWriter(Flags flags) {
        this.dataSource = new AvroDataSource(flags.writeRate, flags.schemaFile);
        this.flags = flags;
    }

    @Override
    void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting kafka writer with config : {}", w.writeValueAsString(flags));
        // create dfs
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", flags.url);
        FileSystem dfs = FileSystem.get(configuration);
        List<Pair<Integer, ParquetWriter<GenericRecord>>> streams = new ArrayList<>(flags.numFiles);
        for (int i = 0; i < flags.numFiles; i++) {
            Path path = new Path(flags.directory + String.format(flags.fileName, i));
            if (!dfs.exists(path)) {
                dfs.create(path, flags.replicaSize);
            }
            ParquetWriter<GenericRecord> writer = null;
            writer = AvroParquetWriter.
                <GenericRecord>builder(path)
                .withRowGroupSize(flags.blockSize)
                .withPageSize(flags.pageSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withSchema(User.getClassSchema())
                .withConf(dfs.getConf())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();
            streams.add(Pair.of(i, writer));
        }
        execute(streams);
    }

    private void execute(List<Pair<Integer, ParquetWriter<GenericRecord>>> fileWriters) throws Exception {
        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<ParquetWriter<GenericRecord>> writersThisThread = fileWriters
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                final long numRecordsForThisThread = flags.numEvents / flags.numThreads;
                final long numBytesForThisThread = flags.numBytes / flags.numThreads;
                executor.submit(() -> {
                    try {
                        write(
                            writersThisThread,
                            numRecordsForThisThread,
                            numBytesForThisThread);
                    } catch (Exception e) {
                        log.error("Encountered error at writing records", e);
                        isDone.set(true);
                        System.exit(-1);
                    }
                });
            }
            log.info("Started {} write threads", flags.numThreads);
            // record start time
            startTime = System.currentTimeMillis();
            // while true loop to output stats
            reportStats();
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    private void write(List<ParquetWriter<GenericRecord>> writers,
                       long numRecordsForThisThread,
                       long numBytesForThisThread) throws Exception {

        log.info("Write thread started with : logs = {},"
                + " num records = {}, num bytes = {}",
            writers.stream().map(l -> l).collect(Collectors.toList()),
            numRecordsForThisThread,
            numBytesForThisThread);

        // Acquire 1 second worth of records to have a slower ramp-up
        RateLimiter.create(flags.writeRate / flags.numThreads).acquire((int) (flags.writeRate / flags.numThreads));

        long totalWritten = 0L;
        long totalBytesWritten = 0L;
        int eventSize = dataSource.getEventSize();
        final int numStream = writers.size();
        while (true) {
            for (int i = 0; i < numStream; i++) {
                if (numRecordsForThisThread > 0
                    && totalWritten >= numRecordsForThisThread) {
                    writers.get(i).close();
                    markPerfDone();
                }
                if (numBytesForThisThread > 0
                    && totalBytesWritten >= numBytesForThisThread) {
                    writers.get(i).close();
                    markPerfDone();
                }
                totalWritten++;
                totalBytesWritten += eventSize;
                if (dataSource.hasNext()) {
                    GenericRecord msg = dataSource.getNext();
                    final long sendTime = System.nanoTime();
                    if(totalWritten % 100000 == 0) {
                        log.info("before write msg");
                        writers.get(i).write(msg);
                        log.info("after write msg");
                    } else {
                        writers.get(i).write(msg);
                    }

                    long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                        System.nanoTime() - sendTime
                    );
                    // these stats can't reflex the data written to fs,
                    // since parquet first cache the data in mem to block_size
                    eventsWritten.increment();
                    bytesWritten.add(eventSize);
                    recorder.recordValue(latencyMicros);
                    cumulativeRecorder.recordValue(latencyMicros);
                    // accumulated stats is more suitable for file scenario
                    cumulativeEventsWritten.increment();
                    cumulativeBytesWritten.add(eventSize);
                }
            }
        }
    }

}
