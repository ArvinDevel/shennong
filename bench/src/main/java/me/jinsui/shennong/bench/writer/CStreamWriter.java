package me.jinsui.shennong.bench.writer;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.schema.Schemas;
import org.apache.bookkeeper.api.schema.TypedSchema;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.StreamSchemaBuilder;
import org.apache.bookkeeper.api.stream.WriteEventBuilder;
import org.apache.bookkeeper.api.stream.WriteResult;
import org.apache.bookkeeper.api.stream.Writer;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.clients.exceptions.StreamExistsException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.IntHashRouter;
import org.apache.bookkeeper.schema.TypedSchemas;
import org.apache.bookkeeper.schema.proto.SchemaInfo;
import org.apache.bookkeeper.schema.proto.SchemaType;
import org.apache.bookkeeper.schema.proto.StructType;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamSchemaInfo;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Write to CStream.
 */
@Slf4j
public class CStreamWriter extends WriterBase {

    /**
     * Flags for the write command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-u", "--url"
            },
            description = "CStream cluster url")
        public String url = "bk://localhost:4181";

        @Parameter(
            names = {
                "-sf", "--schema-file"
            },
            description = "Schema represented as Avro, used in complex mode")
        public String schemaFile = null;

        @Parameter(
            names = {
                "-tn", "--table-name"
            },
            description = "Tpch table name, using tpch data when this specified.")
        public String tableName = null;

        @Parameter(
            names = {
                "-tsf", "--tpch-scale-factor"
            },
            description = "Tpch table generate data scale factor, default 1.")
        public int scaleFactor = 1;

        @Parameter(
            names = {
                "-mbs", "--max-buffer-size"
            },
            description = "Max buffer size in the event set writer, preAllocated max buffer size")
        public int bufferSize = 512 * 1024;

        @Parameter(
            names = {
                "-men", "--max-event-num"
            },
            description = "Max event num in event set (require % 8 == 0)")
        public int maxEventNum = 8192;

        @Parameter(
            names = {
                "-fdms", "--flush-duration-ms"
            },
            description = "Generating event set duration, default 0: disable flush.")
        public int flushDurationMs = 0;

        @Parameter(
            names = {
                "-nn", "--namespace-name"
            },
            description = "Namespace name")
        public String namespaceName = "test-namespace";

        @Parameter(
            names = {
                "-sn", "--stream-name"
            },
            description = "Stream name or stream name pattern if more than 1 stream is specified at `--num-streams`")
        public String streamName = "test-stream-%06d";

        @Parameter(
            names = {
                "-s", "--num-streams"
            },
            description = "Number of log streams")
        public int numStreams = 1;

        @Parameter(
            names = {
                "-inr", "--init-num-ranges"
            },
            description = "Number of init ranges of the stream")
        public int initNumRanges = 1;

        @Parameter(
            names = {
                "-mnr", "--min-num-ranges"
            },
            description = "Number of min ranges of the stream")
        public int minNumRanges = 1;

        @Parameter(
            names = {
                "-t", "--threads"
            },
            description = "Number of threads writing")
        public int numThreads = 1;

    }

    private final Flags flags;

    public CStreamWriter(Flags flags) {
        this.flags = flags;
    }

    @Override
    void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting schema stream storage writer with config : {}", w.writeValueAsString(flags));

        StreamConfiguration streamConf = newStreamConfiguration();
        try (StorageAdminClient adminClient =
                 StorageClientBuilder.newBuilder()
                     .withSettings(StorageClientSettings.newBuilder()
                         .serviceUri(flags.url)
                         .build())
                     .buildAdmin()) {
            NamespaceConfiguration namespaceConfiguration = NamespaceConfiguration.newBuilder()
                .setDefaultStreamConf(DEFAULT_STREAM_CONF)
                .build();

            try {
                FutureUtils.result(adminClient.createNamespace(flags.namespaceName, namespaceConfiguration));
            } catch (NamespaceExistsException cee) {
                // swallow
            } catch (ClientException ce) {
                log.warn("create namespace fail ", ce);
            }
            for (int i = 0; i < flags.numStreams; i++) {
                String streamName = String.format(flags.streamName, i);
                try {
                    FutureUtils.result(adminClient.createStream(flags.namespaceName, streamName, streamConf));
                } catch (StreamExistsException see) {
                    // swallow
                } catch (ClientException ce) {
                    log.warn("create schema stream fail ", ce);
                }
            }
            log.info("Successfully create schema streams, and begin open them");
        }

        try (StorageClient storageClient = StorageClientBuilder.newBuilder()
            .withSettings(StorageClientSettings.newBuilder()
                .serviceUri(flags.url)
                .build())
            .withNamespace(flags.namespaceName)
            .build()) {
            TypedSchema<GenericRecord> valueTypedSchema;
            if (null != flags.tableName) {
                switch (flags.tableName) {
                    case "orders":
                        valueTypedSchema = TypedSchemas.avroSchema(Orders.getClassSchema());
                        break;
                    case "customer":
                        valueTypedSchema = TypedSchemas.avroSchema(Customer.getClassSchema());
                        break;
                    case "lineitem":
                        valueTypedSchema = TypedSchemas.avroSchema(Lineitem.getClassSchema());
                        break;
                    case "part":
                        valueTypedSchema = TypedSchemas.avroSchema(Part.getClassSchema());
                        break;
                    case "partsupp":
                        valueTypedSchema = TypedSchemas.avroSchema(Partsupp.getClassSchema());
                        break;
                    case "supplier":
                        valueTypedSchema = TypedSchemas.avroSchema(Supplier.getClassSchema());
                        break;
                    default:
                        valueTypedSchema = null;
                        System.exit(-1);
                        log.error("{} is Not standard tpch table", flags.tableName);
                }
            } else {
                valueTypedSchema = TypedSchemas.avroSchema(User.getClassSchema());
            }
            StreamConfig<Integer, GenericRecord> streamConfig = StreamConfig.<Integer, GenericRecord>builder()
                .schema(StreamSchemaBuilder.<Integer, GenericRecord>builder()
                    .key(TypedSchemas.int32())
                    .value(valueTypedSchema)
                    .build())
                .keyRouter(IntHashRouter.of())
                .build();
            List<Pair<Integer, Stream<Integer, GenericRecord>>> streams = new ArrayList<>(flags.numStreams);
            for (int i = 0; i < flags.numStreams; i++) {
                String streamName;
                if (-1 != flags.streamOrder) {
                    streamName = String.format(flags.streamName, flags.streamOrder);
                } else {
                    streamName = String.format(flags.streamName, i);
                }
                try {
                    Stream<Integer, GenericRecord> stream = FutureUtils.result(storageClient.openStream(streamName, streamConfig));
                    streams.add(Pair.of(i, stream));
                } catch (StreamNotFoundException snfe) {
                    log.error("stream not found ", snfe);
                    return;
                } catch (Exception ce) {
                    log.error("open stream fail ", ce);
                    return;
                }
            }
            log.info("Successfully open streams, and begin write to them");
            execute(streams);
        }
    }

    private void execute(List<Pair<Integer, Stream<Integer, GenericRecord>>> streams) throws Exception {
        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            final long numRecordsForThisThread = flags.numEvents / flags.numThreads;
            final long numBytesForThisThread = flags.numBytes / flags.numThreads;
            final double writeRateForThisThread = flags.writeRate / flags.numThreads;
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<Stream<Integer, GenericRecord>> logsThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        write(
                            logsThisThread,
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
            startTime = System.currentTimeMillis();
            reportStats();
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            streams.forEach(streamPair -> streamPair.getRight().closeAsync());
        }
    }

    void write(List<Stream<Integer, GenericRecord>> streams,
               double writeRate,
               long numRecordsForThisThread,
               long numBytesForThisThread) throws Exception {
        WriterConfig writerConfig = WriterConfig.builder()
            .maxBufferSize(flags.bufferSize)
            .maxBufferedEvents(flags.maxEventNum)
            .flushDuration(Duration.ofMillis(flags.flushDurationMs)).build();
        List<CompletableFuture<Writer<Integer, GenericRecord>>> writerFutures = streams.stream()
            .map(stream -> stream.openWriter(writerConfig))
            .collect(Collectors.toList());
        List<Writer<Integer, GenericRecord>> writers = result(FutureUtils.collect(writerFutures));

        DataSource<GenericRecord> dataSource;
        if (null != flags.tableName) {
            dataSource = TpchDataSourceFactory.getTblDataSource(writeRate, flags.tableName, flags.scaleFactor);
        } else {
            dataSource = new AvroDataSource(writeRate, flags.schemaFile);
        }

        log.info("Write thread started with : logs = {}, rate = {},"
                + " num records = {}, num bytes = {}",
            streams.stream().map(l -> l.toString()).collect(Collectors.toList()),
            writeRate,
            numRecordsForThisThread,
            numBytesForThisThread);

        int key = 0;


        long totalWritten = 0L;
        long totalBytesWritten = 0L;
        int eventSize = dataSource.getEventSize();
        final int numStream = streams.size();
        while (true) {
            for (int i = 0; i < numStream; i++) {
                if (numRecordsForThisThread > 0
                    && totalWritten >= numRecordsForThisThread) {
                    markPerfDone();
                }
                if (numBytesForThisThread > 0
                    && totalBytesWritten >= numBytesForThisThread) {
                    markPerfDone();
                }

                totalWritten++;
                totalBytesWritten += eventSize;
                if (dataSource.hasNext()) {
                    final long sendTime = System.nanoTime();
                    GenericRecord genericRecord = dataSource.getNext();
                    WriteEventBuilder<Integer, GenericRecord> eventBuilder = writers.get(i).eventBuilder();
                    if (0 != flags.bypass) {
                        eventBuilder.withKey(key++)
                            .withValue(genericRecord)
                            .withTimestamp(System.currentTimeMillis())
                            .build();
                        eventsWritten.increment();
                        bytesWritten.add(eventSize);
                        cumulativeEventsWritten.increment();
                        cumulativeBytesWritten.add(eventSize);

                        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                            System.nanoTime() - sendTime
                        );
                        recorder.recordValue(latencyMicros);
                        cumulativeRecorder.recordValue(latencyMicros);
                    } else {
                        CompletableFuture<WriteResult> eventFuture = writers.get(i).write(eventBuilder.withKey(key++)
                            .withValue(genericRecord)
                            .withTimestamp(System.currentTimeMillis())
                            .build());
                        eventFuture.thenAccept(writeResult -> {

                            eventsWritten.increment();
                            bytesWritten.add(eventSize);
                            cumulativeEventsWritten.increment();
                            cumulativeBytesWritten.add(eventSize);

                            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                                System.nanoTime() - sendTime
                            );
                            recorder.recordValue(latencyMicros);
                            cumulativeRecorder.recordValue(latencyMicros);
                        }).exceptionally(cause -> {
                            log.warn("Error at writing records", cause);
                            isDone.set(true);
                            System.exit(-1);
                            return null;
                        });
                    }
                } else {
                    if (null != flags.tableName) {
                        switch (flags.tableName) {
                            case "orders":
                                if (!((TpchDataSourceFactory.OrdersDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    markPerfDone();
                                }
                                break;
                            case "customer":
                                if (!((TpchDataSourceFactory.CustomerDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    markPerfDone();
                                }
                                break;
                            case "lineitem":
                                if (!((TpchDataSourceFactory.LineitemDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    markPerfDone();
                                }
                                break;
                            case "part":
                                if (!((TpchDataSourceFactory.PartDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    markPerfDone();
                                }
                                break;
                            case "partsupp":
                                if (!((TpchDataSourceFactory.PartsuppDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
                                    markPerfDone();
                                }
                                break;
                            case "supplier":
                                if (!((TpchDataSourceFactory.SupplierDataSource) dataSource).getIterator().hasNext()) {
                                    log.info("Generated orders Tale data were finished, existing...");
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

    private StreamConfiguration newStreamConfiguration() {
        ByteString schema;
        if (null != flags.tableName) {
            switch (flags.tableName) {
                case "orders":
                    schema = Schemas.serializeSchema(Orders.getClassSchema());
                    break;
                case "customer":
                    schema = Schemas.serializeSchema(Customer.getClassSchema());
                    break;
                case "lineitem":
                    schema = Schemas.serializeSchema(Lineitem.getClassSchema());
                    break;
                case "part":
                    schema = Schemas.serializeSchema(Part.getClassSchema());
                    break;
                case "partsupp":
                    schema = Schemas.serializeSchema(Partsupp.getClassSchema());
                    break;
                case "supplier":
                    schema = Schemas.serializeSchema(Supplier.getClassSchema());
                    break;
                default:
                    schema = null;
                    System.exit(-1);
                    log.error("{} is Not standard tpch table", flags.tableName);
            }
        } else {
            schema = Schemas.serializeSchema(User.getClassSchema());
        }
        StreamSchemaInfo streamSchemaInfo = StreamSchemaInfo.newBuilder()
            .setKeySchema(SchemaInfo.newBuilder()
                .setSchemaType(SchemaType.INT32)
                .build())
            .setValSchema(SchemaInfo.newBuilder()
                .setSchemaType(SchemaType.STRUCT)
                .setStructType(StructType.AVRO)
                .setSchema(schema)
                .build())
            .build();

        return StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
            .setSchemaInfo(streamSchemaInfo)
            .setMinNumRanges(flags.minNumRanges)
            .setInitialNumRanges(flags.initNumRanges)
            .build();
    }
}
