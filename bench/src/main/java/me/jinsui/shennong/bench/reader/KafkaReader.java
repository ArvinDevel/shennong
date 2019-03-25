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

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.utils.LineitemDeserializer;
import me.jinsui.shennong.bench.utils.UserDeserializer;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;

/**
 * A perf reader to evaluate read performance to Kafka.
 */
@Slf4j
public class KafkaReader extends ReaderBase {

    /**
     * Flags for the read command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-u", "--url"
            },
            description = "Kafka cluster url")
        public String url = "localhost:9092";

        @Parameter(
            names = {
                "-cp", "--consume-position"
            },
            description = "Consume position, default 0 earliest, -1 latest, others means random choose one offset between earliest and latest")
        public long consumePosition = 0;

        @Parameter(
            names = {
                "-sf", "--schema-file"
            },
            description = "Schema represented using Avro, used in complex mode")
        public String schemaFile = null;

        @Parameter(
            names = {
                "-rc", "--read-column"
            },
            description = "Columns to be read(column stream mode), default value is empty, not check for columns")
        public String readColumn = "";

        @Parameter(
            names = {
                "-tn", "--topic-name"
            },
            description = "Topic name")
        public String topicName = "test-topic-%06d";

        @Parameter(
            names = {
                "-tnum", "--topic-num"
            },
            description = "Topic num")
        public int numTopics = 1;

        @Parameter(
            names = {
                "-t", "--threads"
            },
            description = "Number of threads writing")
        public int numThreads = 1;

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
                "-ttn", "--tpch-table-name"
            },
            description = "Tpch table name, Shall specify correct deserialization util when using tpch data")
        public String tableName = null;

        @Parameter(
            names = {
                "-gid", "--group-id"
            },
            description = "Setting this to coordinate consuming using a consumer group, default using seperated group")
        public String groupId = null;

    }

    protected final Flags flags;

    public KafkaReader(Flags flags) {
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

    private Counter readEventsForPrometheus;
    private Counter readBytes;
    private Summary readLats;

    protected void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting stream perf reader with config : {}", w.writeValueAsString(flags));
        List<Pair<Integer, String>> streams = new ArrayList<>(flags.numTopics);
        for (int i = 0; i < flags.numTopics; i++) {
            String topicName;
            if (-1 != flags.streamOrder) {
                topicName = String.format(flags.topicName, flags.streamOrder);
            } else {
                topicName = String.format(flags.topicName, i);
            }
            streams.add(Pair.of(i, topicName));
        }
        execute(streams);
    }

    private void execute(List<Pair<Integer, String>> streams) throws Exception {
        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<KafkaConsumer> consumersThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .map(topicName -> {
                        KafkaConsumer consumer = new KafkaConsumer<>(newKafkaProperties(flags, topicName));
                        // subscribe to topic
                        consumer.subscribe(Arrays.asList(topicName));
                        return consumer;
                    })
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        if (flags.prometheusEnable) {
                            readWithPrometheusMonitor(consumersThisThread);
                        } else {
                            read(consumersThisThread);
                        }
                    } catch (Exception e) {
                        log.error("Encountered error at reading records", e);
                        isDone.set(true);
                        System.exit(-1);
                    }
                });
            }
            log.info("Started {} read threads", flags.numThreads);
            startTime = System.currentTimeMillis();
            reportStats();
        } catch (Exception e) {
            log.error("Occur exception when put consumer to thread", e);
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    private void read(List<KafkaConsumer> consumersInThisThread) {
        log.info("Read thread started with : topics = {}", consumersInThisThread);

        for (KafkaConsumer consumer : consumersInThisThread) {
            // to get assignment of the consumer
            consumer.poll(flags.pollTimeoutMs);
            log.info("consumer {} has partitions {} ", consumer, consumer.assignment());
            // seek to head
            if (flags.consumePosition == 0) {
                log.info("begin set position to begin");
                consumer.seekToBeginning(consumer.assignment());
                // read from latest
            } else if (flags.consumePosition == -1) {
                log.info("begin set position to end");
                consumer.seekToEnd(consumer.assignment());
            } else {
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(consumer.assignment());
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
                for (TopicPartition topicPartition : (Set<TopicPartition>) consumer.assignment()) {
                    long begin = beginningOffsets.get(topicPartition);
                    long end = endOffsets.get(topicPartition);
                    long offset = ThreadLocalRandom.current().nextLong(begin, end);
                    consumer.seek(topicPartition, offset);
                    log.info("seek topic partition {} offset to {}, begin is {}, end is {}",
                        topicPartition, offset, begin, end);
                }

            }
        }

        String[] readFields =
            Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(flags.readColumn), String.class);
        boolean checkColumn = true;
        if (readFields.length < 1) {
            checkColumn = false;
        } else {
            log.info("Columns to be read are {} ", readFields);
        }
        int backoffNum = 0;
        while (true) {
            for (KafkaConsumer consumer : consumersInThisThread) {
                ConsumerRecords<Long, GenericRecord> records = consumer.poll(flags.pollTimeoutMs);
                if (records.count() == 0 && flags.readEndless == 0) {
                    if (backoffNum > flags.maxBackoffNum) {
                        log.info("No more data after {} ms, shut down", flags.pollTimeoutMs * flags.maxBackoffNum);
                        System.exit(-1);
                    } else {
                        backoffNum++;
                    }
                    continue;
                }
                // reset backoffNum
                backoffNum = 0;
                int num = records.count();
                eventsRead.add(num);
                cumulativeEventsRead.add(num);
                // filter according to read column
                for (ConsumerRecord<Long, GenericRecord> record : records) {
                    if (checkColumn) {
                        for (String field : readFields) {
                            Object data = record.value().get(field);
                        }
                    }
                    // TODO how to estimate field value size
                    int size = record.serializedValueSize() + record.serializedKeySize();
                    bytesRead.add(size);
                    cumulativeBytesRead.add(size);
                }
            }
        }
    }

    private void readWithPrometheusMonitor(List<KafkaConsumer> consumersInThisThread) {
        log.info("Read thread started with : topics = {}", consumersInThisThread);

        // set consume position to head compulsively to avoid can't read from head again after read once
        for (KafkaConsumer consumer : consumersInThisThread) {
            // to get assignment of the consumer
            consumer.poll(flags.pollTimeoutMs);
            log.info("consumer {} has partitions {} ", consumer, consumer.assignment());
            // seek to head
            if (flags.consumePosition == 0) {
                log.info("begin set position to begin");
                consumer.seekToBeginning(consumer.assignment());
                // read from latest
            } else if (flags.consumePosition == -1) {
                log.info("begin set position to end");
                consumer.seekToEnd(consumer.assignment());
            } else {
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(consumer.assignment());
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
                for (TopicPartition topicPartition : (Set<TopicPartition>) consumer.assignment()) {
                    long begin = beginningOffsets.get(topicPartition);
                    long end = endOffsets.get(topicPartition);
                    long offset = ThreadLocalRandom.current().nextLong(begin, end);
                    consumer.seek(topicPartition, offset);
                    log.info("seek topic partition {} offset to {}, begin is {}, end is {}",
                        topicPartition, offset, begin, end);
                }

            }
        }
        String[] readFields =
            Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(flags.readColumn), String.class);
        boolean checkColumn = true;
        if (readFields.length < 1) {
            checkColumn = false;
        } else {
            log.info("Columns to be read are {} ", readFields);
        }
        int backoffNum = 0;
        while (true) {
            for (KafkaConsumer consumer : consumersInThisThread) {
                Summary.Timer requestTimer = readLats.startTimer();
                ConsumerRecords<Long, GenericRecord> records = consumer.poll(flags.pollTimeoutMs);
                if (records.count() == 0 && flags.readEndless == 0) {
                    if (backoffNum > flags.maxBackoffNum) {
                        log.info("No more data after {} ms, shut down", flags.pollTimeoutMs * flags.maxBackoffNum);
                        System.exit(-1);
                    } else {
                        backoffNum++;
                    }
                    continue;
                }
                // reset backoffNum
                backoffNum = 0;
                int num = records.count();
                eventsRead.add(num);
                cumulativeEventsRead.add(num);
                requestTimer.observeDuration();
                readEventsForPrometheus.inc(num);
                // filter according to read column
                for (ConsumerRecord<Long, GenericRecord> record : records) {
                    // TODO how to estimate field value size
                    if (checkColumn) {
                        for (String field : readFields) {
                            Object data = record.value().get(field);
                        }
                    }
                    int size = record.serializedValueSize() + record.serializedKeySize();
                    bytesRead.add(size);
                    cumulativeBytesRead.add(size);
                    readBytes.inc(size);
                }
            }
        }
    }

    private Properties newKafkaProperties(Flags flags, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", flags.url);
        props.put("group.id", topicName);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        if (null != flags.groupId) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, flags.groupId);
        } else {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        }
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        if (null != flags.tableName) {
            switch (flags.tableName) {
                case "orders":
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);
                    break;
                case "customer":
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);
                    break;
                case "lineitem":
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LineitemDeserializer.class);
                    break;
                case "part":
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);

                    break;
                case "partsupp":
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);

                    break;
                case "supplier":
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);

                    break;
                default:
                    log.error("Wrong tpch tbl name {} !", flags.tableName);
                    System.exit(-1);
            }
        } else {
            // custom user schema
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);
        }
        if (flags.consumePosition == 0) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        return props;
    }
}
