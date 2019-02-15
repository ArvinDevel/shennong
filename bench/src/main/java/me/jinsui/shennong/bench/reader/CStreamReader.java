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
import me.jinsui.shennong.bench.avro.User;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.generic.GenericRecord;
import org.apache.bookkeeper.api.StorageClient;
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
                "-sf", "--schema-file"
            },
            description = "Schema represented using Avro, used in complex mode")
        public String schemaFile = null;

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
                "-mr", "--max-readahead-records"
            },
            description = "Max readhead records")
        public int maxReadAheadRecords = 1000000;

        @Parameter(
            names = {
                "-ns", "--num-splits-per-segment"
            },
            description = "Num splits per segment")
        public int numSplitsPerSegment = 1;

        @Parameter(
            names = {
                "-cs", "--readahead-cache-size"
            },
            description = "ReadAhead Cache Size, in bytes"
        )
        public int readAheadCatchSize = 8 * 1024 * 1024;
    }

    protected final Flags flags;

    public CStreamReader(Flags flags) {
        this.flags = flags;
    }

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running schema stream storage reader", e);
        }
    }

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
            StreamConfig<Integer, GenericRecord> streamConfig = StreamConfig.<Integer, GenericRecord>builder()
                .schema(StreamSchemaBuilder.<Integer, GenericRecord>builder()
                    .key(TypedSchemas.int32())
                    .value(TypedSchemas.avroSchema(User.getClassSchema()))
                    .build())
                .keyRouter(IntHashRouter.of())
                .build();
            List<Pair<Integer, Stream<Integer, GenericRecord>>> streams = new ArrayList<>(flags.numStreams);
            for (int i = 0; i < flags.numStreams; i++) {
                String streamName = String.format(flags.streamName, i);
                try {
                    Stream<Integer, GenericRecord> stream =
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

    private void execute(List<Pair<Integer, Stream<Integer, GenericRecord>>> streams, int mode) throws Exception {
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
                final List<Stream<Integer, GenericRecord>> logsThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        if (0 == mode) {
                            read(logsThisThread);
                        } else if (1 == mode) {
                            readColumn(logsThisThread);
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
    void read(List<Stream<Integer, GenericRecord>> streams) throws Exception {
        ReaderConfig readerConfig = ReaderConfig.builder()
            .maxReadAheadCacheSize(flags.readAheadCatchSize).build();
        List<CompletableFuture<Reader<Integer, GenericRecord>>> readerFutures = streams.stream()
            .map(stream -> stream.openReader(readerConfig, Position.HEAD))
            .collect(Collectors.toList());
        List<Reader<Integer, GenericRecord>> readers = result(FutureUtils.collect(readerFutures));
        log.info("Read thread started with : logs = {}",
            streams.stream().map(stream -> stream.toString()).collect(Collectors.toList()));

        final int numLogs = streams.size();
        ReadEvents<Integer, GenericRecord> readEvents;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                readEvents = readers.get(i).readNext(100, TimeUnit.MILLISECONDS);
                if (null != readEvents) {
                    final long receiveTime = System.currentTimeMillis();
                    eventsRead.add(readEvents.numEvents());
                    bytesRead.add(readEvents.getEstimatedSize());
                    cumulativeEventsRead.add(readEvents.numEvents());
                    cumulativeBytesRead.add(readEvents.getEstimatedSize());

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
                        if (((EventPositionImpl) readEvent.position()).getRangeSeqNum() % 1000 == 0
                            && ((EventPositionImpl) readEvent.position()).getSlotId() % 100 == 0) {
                            log.info("Read event @ position {}, ts {}",
                                readEvent.position(), readEvent.timestamp());
                        }
                    }
                }
            }
        }
    }

    void readColumn(List<Stream<Integer, GenericRecord>> streams) throws Exception {
        ArrayList<String> list = new ArrayList<String>(Arrays.asList(flags.readColumn.split(",")));
        log.info("Columns to be read is:");
        for (String column : list) {
            log.info("{}", column);
        }
        ColumnReaderConfig readerConfig = ColumnReaderConfig.builder()
            .columns(list)
            .maxReadAheadCacheSize(flags.readAheadCatchSize).build();
        List<CompletableFuture<ColumnReader<Integer, GenericRecord>>> readerFutures = streams.stream()
            .map(stream -> stream.openColumnReader(readerConfig, Position.HEAD, Position.TAIL))
            .collect(Collectors.toList());
        List<ColumnVectors<Integer, GenericRecord>> columnVectorsList;
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
        ColumnVectors<Integer, GenericRecord> columnVectors;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                columnVectors = columnVectorsList.get(i);
                if (null != columnVectors) {
                    if (columnVectors.hasNext()) {
                        ColumnVector columnVector = columnVectors.next(100, TimeUnit.MILLISECONDS);
                        if (null != columnVector) {
                            cumulativeEventsRead.increment();
                            cumulativeBytesRead.add(columnVector.estimatedSize());
                            eventsRead.increment();
                            bytesRead.add(columnVector.estimatedSize());
                            if (((EventPositionImpl) columnVector.position()).getRangeSeqNum() % 1000 == 0) {
                                log.info("Column vector's stream is {}, end position is {} ",
                                    columnVector.stream(), columnVector.position());
                            }
                        }
                    }
                }
            }
        }
    }
}
