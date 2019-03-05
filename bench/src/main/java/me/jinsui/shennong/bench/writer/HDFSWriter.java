package me.jinsui.shennong.bench.writer;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.avro.Customer;
import me.jinsui.shennong.bench.avro.Lineitem;
import me.jinsui.shennong.bench.avro.Orders;
import me.jinsui.shennong.bench.avro.Part;
import me.jinsui.shennong.bench.avro.Partsupp;
import me.jinsui.shennong.bench.avro.Supplier;
import me.jinsui.shennong.bench.avro.User;
import me.jinsui.shennong.bench.source.AvroDataSource;
import me.jinsui.shennong.bench.source.DataSource;
import me.jinsui.shennong.bench.source.TpchDataSourceFactory;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.Schema;
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
public class HDFSWriter extends WriterBase {

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

        @Parameter(
            names = {
                "-tn", "--table-name"
            },
            description = "Tpch table name, using tpch data source when this specified.")
        public String tableName = null;

        @Parameter(
            names = {
                "-tsf", "--tpch-scale-factor"
            },
            description = "Tpch table generate data scale factor, default 1.")
        public int scaleFactor = 1;

    }

    private final Flags flags;

    public HDFSWriter(Flags flags) {
        this.flags = flags;
    }

    @Override
    void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting parquet writer over HDFS with config : {}", w.writeValueAsString(flags));
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
            Schema writerSchema;
            if (null != flags.tableName) {
                switch (flags.tableName) {
                    case "orders":
                        writerSchema = Orders.getClassSchema();
                        break;
                    case "lineitem":
                        writerSchema = Lineitem.getClassSchema();
                        break;
                    case "customer":
                        writerSchema = Customer.getClassSchema();
                        break;
                    case "part":
                        writerSchema = Part.getClassSchema();
                        break;
                    case "partsupp":
                        writerSchema = Partsupp.getClassSchema();
                        break;
                    case "supplier":
                        writerSchema = Supplier.getClassSchema();
                        break;
                    default:
                        writerSchema = null;
                        System.exit(-1);
                        log.error("{} is Not standard tpch table", flags.tableName);
                }
            } else {
                writerSchema = User.getClassSchema();
            }
            ParquetWriter<GenericRecord> writer = null;
            writer = AvroParquetWriter.
                <GenericRecord>builder(path)
                .withRowGroupSize(flags.blockSize)
                .withPageSize(flags.pageSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withSchema(writerSchema)
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
            final long numRecordsForThisThread = flags.numEvents / flags.numThreads;
            final long numBytesForThisThread = flags.numBytes / flags.numThreads;
            final double writeRateForThisThread = flags.writeRate / (double) flags.numThreads;
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<ParquetWriter<GenericRecord>> writersThisThread = fileWriters
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        write(
                            writersThisThread,
                            writeRateForThisThread,
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
                       double writeRate,
                       long numRecordsForThisThread,
                       long numBytesForThisThread) throws Exception {

        DataSource<GenericRecord> dataSource;
        if (null != flags.tableName) {
            dataSource = TpchDataSourceFactory.getTblDataSource(writeRate, flags.tableName, flags.scaleFactor);
        } else {
            dataSource = new AvroDataSource(writeRate, flags.schemaFile);
        }

        log.info("Write thread started with : logs = {},"
                + " num records = {}, num bytes = {}",
            writers.stream().map(l -> l).collect(Collectors.toList()),
            numRecordsForThisThread,
            numBytesForThisThread);


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
                    writers.get(i).write(msg);

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
                } else {
                    if (null != flags.tableName) {
                        // as tpch factor restricted total data, so terminate program
                        switch (flags.tableName) {
                            case "orders":
                                if (!((TpchDataSourceFactory.OrdersDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    writers.get(i).close();
                                    markPerfDone();
                                }
                                break;
                            case "lineitem":
                                if (!((TpchDataSourceFactory.LineitemDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    writers.get(i).close();
                                    markPerfDone();
                                }
                                break;
                            case "customer":
                                if (!((TpchDataSourceFactory.CustomerDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    writers.get(i).close();
                                    markPerfDone();
                                }
                                break;
                            case "part":
                                if (!((TpchDataSourceFactory.PartDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    writers.get(i).close();
                                    markPerfDone();
                                }
                                break;
                            case "partsupp":
                                if (!((TpchDataSourceFactory.PartsuppDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    writers.get(i).close();
                                    markPerfDone();
                                }
                                break;
                            case "supplier":
                                if (!((TpchDataSourceFactory.SupplierDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    writers.get(i).close();
                                    markPerfDone();
                                }
                                break;
                            default:
                                log.error("Shouldn't come to here!");
                                System.exit(-1);
                        }
                    }
                }
            }
        }
    }

}
