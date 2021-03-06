/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.jinsui.shennong.bench.reader;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import me.jinsui.shennong.bench.source.CustomDataSource;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.generic.GenericRecord;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.schema.TypedSchema;
import org.apache.bookkeeper.api.stream.ColumnReader;
import org.apache.bookkeeper.api.stream.ColumnReaderConfig;
import org.apache.bookkeeper.api.stream.ColumnVector;
import org.apache.bookkeeper.api.stream.ColumnVectors;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.StreamSchemaBuilder;
import org.apache.bookkeeper.api.stream.exceptions.StreamApiException;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.RangePositionImpl;
import org.apache.bookkeeper.clients.impl.stream.utils.PositionUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.common.router.BytesHashRouter;
import org.apache.bookkeeper.common.router.IntHashRouter;
import org.apache.bookkeeper.schema.TypedSchemas;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A perf reader to evaluate read performance to cstream.
 */
@Slf4j
public class CStreamReader extends ReaderBase {

    /**
     * Flags for the read command.
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
                "-m", "--read-mode"
            },
            description = "Read mode, 0 indicate stream read, 1 indicate column stream read, default 0"
        )
        public int readMode = 0;

        @Parameter(
            names = {
                "-pf", "--position-file"
            },
            description = "Local file to store start position of stream")
        public String startPositionFile = "position-file.binary";

        @Parameter(
            names = {
                "-rc", "--read-column"
            },
            description = "Columns to be read(column stream mode), default value is for default avro schema")
        public String readColumn = "age";

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
            description = "Number of threads reading")
        public int numThreads = 1;

        @Parameter(
            names = {
                "-cs", "--readahead-cache-size"
            },
            description = "ReadAhead Cache Size, in bytes"
        )
        public int readAheadCatchSize = 8 * 1024 * 1024;

        @Parameter(
            names = {
                "-pt", "--poll-timeout-ms"
            },
            description = "Timeout of consumer poll")
        public long pollTimeoutMs = 100;

        @Parameter(
            names = {
                "-re", "--read-endless"
            },
            description = "Whether read endless or not, default 1/true, set to 0/false to stats ")
        public int readEndless = 1;

        @Parameter(
            names = {
                "-mbn", "--max-backoff-num"
            },
            description = "Max backoff number")
        public int maxBackoffNum = -1;

        @Parameter(
            names = {
                "-tn", "--table-name"
            },
            description = "Read data from Tpch table.")
        public String tableName = null;
    }

    protected final Flags flags;

    public CStreamReader(Flags flags) {
        this.flags = flags;
        if (flags.prometheusEnable) {
            startPrometheusServer(flags.prometheusPort);
            readEventsForPrometheus =
                Counter.build()
                    .name("read_requests_finished_total")
                    .help("Total read requests.")
                    .register();
            readBytes =
                Counter.build()
                    .name("read_bytes_finished_total")
                    .help("Total read bytes.")
                    .register();
            readLats =
                Summary.build()
                    .name("request_duration_seconds")
                    .help("Total read latencies.")
                    .register();
        }
    }

    private static Counter readEventsForPrometheus;
    private static Counter readBytes;
    private static Summary readLats;

    protected void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting stream perf reader with config : {}", w.writeValueAsString(flags));

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
                if (null != flags.schemaFile) {
                    valueTypedSchema = TypedSchemas.avroSchema(
                        new CustomDataSource(1, flags.schemaFile, 1).getSchema());
                } else {
                    valueTypedSchema = TypedSchemas.avroSchema(User.getClassSchema());
                }
            }
            StreamConfig<byte[], GenericRecord> streamConfig = StreamConfig.<byte[], GenericRecord>builder()
                .schema(StreamSchemaBuilder.<byte[], GenericRecord>builder()
                    .key(TypedSchemas.bytes())
                    .value(valueTypedSchema)
                    .build())
                .keyRouter(BytesHashRouter.of())
                .build();
            List<Pair<Integer, Stream<byte[], GenericRecord>>> streams = new ArrayList<>(flags.numStreams);
            for (int i = 0; i < flags.numStreams; i++) {
                String streamName;
                if (-1 != flags.streamOrder) {
                    streamName = String.format(flags.streamName, flags.streamOrder);
                } else {
                    streamName = String.format(flags.streamName, i);
                }
                try {
                    Stream<byte[], GenericRecord> stream =
                        FutureUtils.result(storageClient.openStream(streamName, streamConfig));
                    streams.add(Pair.of(i, stream));
                } catch (StreamNotFoundException snfe) {
                    log.error("stream not found ", snfe);
                    return;
                } catch (Exception ce) {
                    log.error("open stream fail ", ce);
                    return;
                }
            }
            if (0 == flags.readMode || 1 == flags.readMode) {
                log.info("Successfully open streams, and begin read from them");
                execute(streams, flags.readMode);
            } else {
                log.error("Unsupported read mode");
            }
        }
    }

    private void execute(List<Pair<Integer, Stream<byte[], GenericRecord>>> streams, int mode) throws Exception {
        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        if (1 == mode) {
            log.info("Begin read column from stream");
        }

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<Stream<byte[], GenericRecord>> logsThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        if (flags.prometheusEnable) {
                            if (0 == mode) {
                                readWithPrometheusMonitor(logsThisThread);
                            } else if (1 == mode) {
                                readColumnWithPrometheusMonitor(logsThisThread);
                            }
                        } else {
                            if (0 == mode) {
                                read(logsThisThread);
                            } else if (1 == mode) {
                                readColumn(logsThisThread);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Encountered error at reading records", e);
                        System.exit(-1);
                    }
                });
            }
            log.info("Started {} read threads", flags.numThreads);
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

    // read used by specific thread
    void read(List<Stream<byte[], GenericRecord>> streams) throws Exception {
        ReaderConfig readerConfig = ReaderConfig.builder()
            .maxReadAheadCacheSize(flags.readAheadCatchSize).build();
        List<CompletableFuture<Reader<byte[], GenericRecord>>> readerFutures = streams.stream()
            .map(stream -> stream.openReader(readerConfig, Position.HEAD))
            .collect(Collectors.toList());
        List<Reader<byte[], GenericRecord>> readers = result(FutureUtils.collect(readerFutures));
        log.info("Read thread started with : logs = {}",
            streams.stream().map(stream -> stream.toString()).collect(Collectors.toList()));

        final int numLogs = streams.size();
        ReadEvents<byte[], GenericRecord> readEvents;
        int backoffNum = 0;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                readEvents = readers.get(i).readNext(flags.pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (null != readEvents) {
                    final long receiveTime = System.currentTimeMillis();
                    int num = readEvents.numEvents();
                    int size = readEvents.getEstimatedSize();

                    eventsRead.add(num);
                    bytesRead.add(size);
                    cumulativeEventsRead.add(num);
                    cumulativeBytesRead.add(size);

                    for (int j = 0; j < readEvents.numEvents(); j++) {
                        ReadEvent readEvent = readEvents.next();
                        long latencyMilli = receiveTime - readEvent.timestamp();
                        try {
                            recorder.recordValue(latencyMilli);
                            cumulativeRecorder.recordValue(latencyMilli);
                        } catch (ArrayIndexOutOfBoundsException oobe) {
                            log.error("receiveTime is {}, readEvent.timestamp() is {}",
                                receiveTime, readEvent.timestamp());
                        }
                    }
                    // reset backoffNum
                    backoffNum = 0;
                } else if (flags.readEndless == 0) {
                    if (backoffNum > flags.maxBackoffNum) {
                        log.info("No more data after {} ms, shut down", flags.pollTimeoutMs * flags.maxBackoffNum);
                        System.exit(-1);
                    } else {
                        backoffNum++;
                    }
                }
            }
        }
    }

    void readColumn(List<Stream<byte[], GenericRecord>> streams) throws Exception {
        String[] readFields =
            Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(flags.readColumn), String.class);
        ArrayList<String> list = new ArrayList<>(Arrays.asList(readFields));
        log.info("Columns to be read is:");
        for (String column : list) {
            log.info("{}", column);
        }
        ColumnReaderConfig readerConfig = ColumnReaderConfig.builder()
            .columns(list)
            .maxReadAheadCacheSize(flags.readAheadCatchSize).build();
        List<CompletableFuture<ColumnReader<byte[], GenericRecord>>> readerFutures = streams.stream()
            .map(stream -> stream.openColumnReader(readerConfig, Position.HEAD, Position.TAIL))
            .collect(Collectors.toList());
        List<ColumnVectors<byte[], GenericRecord>> columnVectorsList;
        columnVectorsList = result(FutureUtils.collect(readerFutures)).stream()
            .map(columnReader -> {
                    try {
                        return columnReader.readNextVector();
                    } catch (StreamApiException sae) {
                        log.error("Read column vector fail ", sae);
                        return null;
                    }
                }
            ).collect(Collectors.toList());

        log.info("Read thread started with : logs = {}",
            streams.stream().map(stream -> stream.toString()).collect(Collectors.toList()));

        final int numLogs = streams.size();
        ColumnVectors<byte[], GenericRecord> columnVectors;
        int backoffNum = 0;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                columnVectors = columnVectorsList.get(i);
                if (null != columnVectors) {
                    if (columnVectors.hasNext()) {
                        ColumnVector columnVector = columnVectors.next(flags.pollTimeoutMs, TimeUnit.MILLISECONDS);
                        if (null != columnVector) {
                            int num = columnVector.num();
                            int size = columnVector.estimatedSize();
                            cumulativeEventsRead.add(num);
                            cumulativeBytesRead.add(size);
                            eventsRead.add(num);
                            bytesRead.add(size);
                            // reset backoffNum
                            backoffNum = 0;
                        } else if (flags.readEndless == 0) {
                            if (backoffNum > flags.maxBackoffNum) {
                                log.info("No more data after continurous {} ms, shut down", flags.pollTimeoutMs * flags.maxBackoffNum);
                                System.exit(-1);
                            } else {
                                backoffNum++;
                            }
                        }
                    }
                }
            }
        }
    }

    // read monitered through prometheus stats
    void readWithPrometheusMonitor(List<Stream<byte[], GenericRecord>> streams) throws Exception {
        ReaderConfig readerConfig = ReaderConfig.builder()
            .maxReadAheadCacheSize(flags.readAheadCatchSize).build();
        List<CompletableFuture<Reader<byte[], GenericRecord>>> readerFutures = streams.stream()
            .map(stream -> stream.openReader(readerConfig, Position.HEAD))
            .collect(Collectors.toList());
        List<Reader<byte[], GenericRecord>> readers = result(FutureUtils.collect(readerFutures));
        log.info("Read thread started with : logs = {}",
            streams.stream().map(stream -> stream.toString()).collect(Collectors.toList()));

        final int numLogs = streams.size();
        ReadEvents<byte[], GenericRecord> readEvents;
        int backoffNum = 0;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                Summary.Timer requestTimer = readLats.startTimer();
                readEvents = readers.get(i).readNext(flags.pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (null != readEvents) {
                    final long receiveTime = System.currentTimeMillis();
                    int num = readEvents.numEvents();
                    int size = readEvents.getEstimatedSize();
                    requestTimer.observeDuration();
                    readEventsForPrometheus.inc(num);
                    readBytes.inc(size);

                    eventsRead.add(num);
                    bytesRead.add(size);
                    cumulativeEventsRead.add(num);
                    cumulativeBytesRead.add(size);

                    for (int j = 0; j < readEvents.numEvents(); j++) {
                        ReadEvent readEvent = readEvents.next();
                        long latencyMilli = receiveTime - readEvent.timestamp();
                        try {
                            recorder.recordValue(latencyMilli);
                            cumulativeRecorder.recordValue(latencyMilli);
                        } catch (ArrayIndexOutOfBoundsException oobe) {
                            log.error("receiveTime is {}, readEvent.timestamp() is {}",
                                receiveTime, readEvent.timestamp());
                        }
                    }
                    // reset backoffNum
                    backoffNum = 0;
                } else if (flags.readEndless == 0) {
                    if (backoffNum > flags.maxBackoffNum) {
                        log.info("No more data after {} ms, shut down", flags.pollTimeoutMs * flags.maxBackoffNum);
                        System.exit(-1);
                    } else {
                        backoffNum++;
                    }
                }
            }
        }
    }

    void readColumnWithPrometheusMonitor(List<Stream<byte[], GenericRecord>> streams) throws Exception {
        String[] readFields =
            Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(flags.readColumn), String.class);
        ArrayList<String> list = new ArrayList<String>(Arrays.asList(readFields));
        log.info("Columns to be read is:");
        for (String column : list) {
            log.info("{}", column);
        }
        ColumnReaderConfig readerConfig = ColumnReaderConfig.builder()
            .columns(list)
            .maxReadAheadCacheSize(flags.readAheadCatchSize).build();
        List<CompletableFuture<ColumnReader<byte[], GenericRecord>>> readerFutures = streams.stream()
            .map(stream -> stream.openColumnReader(readerConfig, Position.HEAD, Position.TAIL))
            .collect(Collectors.toList());
        List<ColumnVectors<byte[], GenericRecord>> columnVectorsList;
        columnVectorsList = result(FutureUtils.collect(readerFutures)).stream()
            .map(columnReader -> {
                    try {
                        return columnReader.readNextVector();
                    } catch (StreamApiException sae) {
                        log.error("Read column vector fail ", sae);
                        return null;
                    }
                }
            ).collect(Collectors.toList());

        log.info("Read thread started with : logs = {}",
            streams.stream().map(stream -> stream.toString()).collect(Collectors.toList()));

        final int numLogs = streams.size();
        ColumnVectors<byte[], GenericRecord> columnVectors;
        int backoffNum = 0;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                columnVectors = columnVectorsList.get(i);
                if (null != columnVectors) {
                    if (columnVectors.hasNext()) {
                        Summary.Timer requestTimer = readLats.startTimer();
                        ColumnVector columnVector = columnVectors.next(flags.pollTimeoutMs, TimeUnit.MILLISECONDS);
                        if (null != columnVector) {
                            int num = columnVector.num();
                            int size = columnVector.estimatedSize();
                            requestTimer.observeDuration();
                            readEventsForPrometheus.inc(num);
                            readBytes.inc(size);
                            cumulativeEventsRead.add(num);
                            cumulativeBytesRead.add(size);
                            eventsRead.add(num);
                            bytesRead.add(size);
                            if (((EventPositionImpl) columnVector.position()).getRangeSeqNum() % 1000 == 0) {
                                log.info("Column vector's stream is {}, end position is {} ",
                                    columnVector.stream(), columnVector.position());
                            }
                            // reset backoffNum
                            backoffNum = 0;
                        } else if (flags.readEndless == 0) {
                            if (backoffNum > flags.maxBackoffNum) {
                                log.info("No more data after continurous {} ms, shut down", flags.pollTimeoutMs * flags.maxBackoffNum);
                                System.exit(-1);
                            } else {
                                backoffNum++;
                            }
                        }
                    }
                }
            }
        }
    }
}
