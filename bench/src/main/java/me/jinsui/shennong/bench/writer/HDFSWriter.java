package me.jinsui.shennong.bench.writer;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
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
import me.jinsui.shennong.bench.source.CustomDataSource;
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
        public int replicaSize = 3;

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
        if (flags.prometheusEnable) {
            startPrometheusServer(flags.prometheusPort);
            writtenEvents =
                Counter.build()
                    .name("write_requests_finished_total")
                    .help("Total write requests.")
                    .register();
            writtenBytes =
                Counter.build()
                    .name("write_bytes_finished_total")
                    .help("Total write bytes.")
                    .register();
            writtenLats =
                Summary.build()
                    .quantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .quantile(0.75, 0.01)
                    .quantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .quantile(0.99, 0.001)
                    .quantile(0.999, 0.0001)
                    .name("request_duration_seconds")
                    .help("Total write latencies.")
                    .register();
        }
    }

    private static Counter writtenEvents;
    private static Counter writtenBytes;
    private static Summary writtenLats;

    @Override
    protected void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting parquet writer over HDFS with config : {}", w.writeValueAsString(flags));
        // create dfs
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", flags.url);
        FileSystem dfs = FileSystem.get(configuration);
        List<Pair<Integer, ParquetWriter<GenericRecord>>> streams = new ArrayList<>(flags.numFiles);
        for (int i = 0; i < flags.numFiles; i++) {
            String fileName;
            if (-1 != flags.streamOrder) {
                fileName = String.format(flags.fileName, flags.streamOrder);
            } else {
                fileName = String.format(flags.fileName, i);
            }
            Path path = new Path(flags.directory + fileName);
            if (!dfs.exists(path)) {
                dfs.create(path, (short) flags.replicaSize);
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
            } else if (null != flags.schemaFile) {
                writerSchema = new CustomDataSource(1, flags.schemaFile, 1).getSchema();
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
            // close to avoid parquet info not written
            fileWriters
                .stream()
                .map(pair -> {
                    try {
                        pair.getRight().close();
                    } catch (Exception e) {
                        // Note that: once this occur, the parquet file can' tbe recognized, so it's better specify event num/bytes to write!
                        log.error("fail to close writer", e);
                    }
                    return null;
                });
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
                        if (flags.prometheusEnable) {
                            writeWithPrometheusMonitor(
                                writersThisThread,
                                writeRateForThisThread,
                                numRecordsForThisThread,
                                numBytesForThisThread);
                        } else {
                            write(
                                writersThisThread,
                                writeRateForThisThread,
                                numRecordsForThisThread,
                                numBytesForThisThread);
                        }
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
        } else if (null != flags.schemaFile) {
            dataSource = new CustomDataSource(writeRate, flags.schemaFile, flags.bytesSize);
        } else {
            dataSource = new AvroDataSource(writeRate);
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
                    final long sendTime = System.nanoTime();
                    GenericRecord msg = dataSource.getNext();
                    if (0 == flags.bypass) {
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

    private void writeWithPrometheusMonitor(List<ParquetWriter<GenericRecord>> writers,
                                            double writeRate,
                                            long numRecordsForThisThread,
                                            long numBytesForThisThread) throws Exception {

        DataSource<GenericRecord> dataSource;
        if (null != flags.tableName) {
            dataSource = TpchDataSourceFactory.getTblDataSource(writeRate, flags.tableName, flags.scaleFactor);
        } else if (null != flags.schemaFile) {
            dataSource = new CustomDataSource(writeRate, flags.schemaFile, flags.bytesSize);
        } else {
            dataSource = new AvroDataSource(writeRate);
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
                    final long sendTime = System.nanoTime();
                    GenericRecord msg = dataSource.getNext();
                    Summary.Timer requestTimer = writtenLats.startTimer();
                    if (0 == flags.bypass) {
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
                    requestTimer.observeDuration();
                    writtenBytes.inc();
                    writtenEvents.inc(eventSize);
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
