package me.jinsui.shennong.bench.writer;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.schema.Schemas;
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
public class CStreamWriter extends me.jinsui.shennong.bench.writer.Writer {

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
                "-r", "--rate"
            },
            description = "Write rate bytes/s across log streams")
        public int writeRate = 0;

        @Parameter(
            names = {
                "-sf", "--schema-file"
            },
            description = "Schema represented as Avro, used in complex mode")
        public String schemaFile = null;

        @Parameter(
            names = {
                "-mbs", "--max-bytes-size"
            },
            description = "Max bytes size in the event")
        public int bytesSize = 8;

        @Parameter(
            names = {
                "-men", "--max-event-num"
            },
            description = "Max event num in event set (require % 8 == 0)")
        public int maxEventNum = 256;

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
                "-e", "--ensemble-size"
            },
            description = "Ledger ensemble size")
        public int ensembleSize = 1;

        @Parameter(
            names = {
                "-w", "--write-quorum-size"
            },
            description = "Ledger write quorum size")
        public int writeQuorumSize = 1;

        @Parameter(
            names = {
                "-a", "--ack-quorum-size"
            },
            description = "Ledger ack quorum size")
        public int ackQuorumSize = 1;

    }

    private final DataSource<GenericRecord> dataSource;
    private final Flags flags;

    public CStreamWriter(Flags flags) {
        this.dataSource = new AvroDataSource(flags.writeRate, flags.schemaFile);
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
            try {
                for (int i = 0; i < flags.numStreams; i++) {
                    String streamName = String.format(flags.streamName, i);
                    FutureUtils.result(adminClient.createStream(flags.namespaceName, streamName, streamConf));
                }
            } catch (StreamExistsException see) {
                // swallow
            } catch (ClientException ce) {
                log.warn("create schema stream fail ", ce);
            }
            log.info("Successfully create schema streams, and begin open them");
        }

        try (StorageClient storageClient = StorageClientBuilder.newBuilder()
            .withSettings(StorageClientSettings.newBuilder()
                .serviceUri(flags.url)
                .build())
            .withNamespace(flags.namespaceName)
            .build()) {
            StreamConfig<Integer, GenericRecord> streamConfig = StreamConfig.<Integer, GenericRecord>builder()
                .schema(StreamSchemaBuilder.<Integer, GenericRecord>builder()
                    .key(TypedSchemas.int32())
                    .value(TypedSchemas.avroSchema(User.getClassSchema()))
                    .build())
                .keyRouter(IntHashRouter.of())
                .build();
            List<Pair<Integer, Stream<Integer, GenericRecord>>> streams = new ArrayList<>(flags.numStreams);
            try {
                for (int i = 0; i < flags.numStreams; i++) {
                    String streamName = String.format(flags.streamName, i);
                    Stream<Integer, GenericRecord> stream = FutureUtils.result(storageClient.openStream(streamName, streamConfig));
                    streams.add(Pair.of(i, stream));
                }
            } catch (StreamNotFoundException snfe) {
                log.error("stream not found ", snfe);
                return;
            } catch (Exception ce) {
                log.error("open stream fail ", ce);
                return;
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
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<Stream<Integer, GenericRecord>> logsThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                final long numRecordsForThisThread = flags.numEvents / flags.numThreads;
                final long numBytesForThisThread = flags.numBytes / flags.numThreads;
                final double writeRateForThisThread = flags.writeRate / (double) flags.numThreads;
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
        WriterConfig writerConfig = WriterConfig.builder().build();
        List<CompletableFuture<Writer<Integer, GenericRecord>>> writerFutures = streams.stream()
            .map(stream -> stream.openWriter(writerConfig))
            .collect(Collectors.toList());
        List<Writer<Integer, GenericRecord>> writers = result(FutureUtils.collect(writerFutures));

        log.info("Write thread started with : logs = {}, rate = {},"
                + " num records = {}, num bytes = {}",
            streams.stream().map(l -> l.toString()).collect(Collectors.toList()),
            writeRate,
            numRecordsForThisThread,
            numBytesForThisThread);

        int key = 0;

        // Acquire 1 second worth of records to have a slower ramp-up
        RateLimiter.create(flags.writeRate / flags.numThreads).acquire((int) (flags.writeRate / flags.numThreads));

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
                    GenericRecord genericRecord = dataSource.getNext();
                    final long sendTime = System.nanoTime();
                    WriteEventBuilder<Integer, GenericRecord> eventBuilder = writers.get(i).eventBuilder();
                    CompletableFuture<WriteResult> eventFuture = writers.get(i).write(eventBuilder.withKey(key++)
                        .withValue(genericRecord)
                        .withTimestamp(sendTime)
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
            }
        }
    }

    private static StreamConfiguration newStreamConfiguration() {
        StreamSchemaInfo streamSchemaInfo = StreamSchemaInfo.newBuilder()
            .setKeySchema(SchemaInfo.newBuilder()
                .setSchemaType(SchemaType.INT32)
                .build())
            .setValSchema(SchemaInfo.newBuilder()
                .setSchemaType(SchemaType.STRUCT)
                .setStructType(StructType.AVRO)
                .setSchema(Schemas.serializeSchema(User.getClassSchema()))
                .build())
            .build();

        return StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
            .setSchemaInfo(streamSchemaInfo)
            .build();
    }
}
