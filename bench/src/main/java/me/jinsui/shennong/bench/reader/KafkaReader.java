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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.utils.AvroDeserializer;
import me.jinsui.shennong.bench.utils.AvroSerializer;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

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
                "-sf", "--schema-file"
            },
            description = "Schema represented using Avro, used in complex mode")
        public String schemaFile = null;

        @Parameter(
            names = {
                "-rc", "--read-column"
            },
            description = "Columns to be read(column stream mode), default value is for default avro schema")
        public String readColumn = "age";

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

    }

    protected final Flags flags;

    public KafkaReader(Flags flags) {
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
        List<Pair<Integer, String>> streams = new ArrayList<>(flags.numTopics);
        for (int i = 0; i < flags.numTopics; i++) {
            String topicName = String.format(flags.topicName, i);
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
                        read(consumersThisThread);
                    } catch (Exception e) {
                        log.error("Encountered error at writing records", e);
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
        log.info("Read thread started with : topics = {},", consumersInThisThread);

        while (true) {
            for (KafkaConsumer consumer : consumersInThisThread) {
                ConsumerRecords<Long, GenericRecord> records = consumer.poll(flags.pollTimeoutMs);
                eventsRead.add(records.count());
                cumulativeEventsRead.add(records.count());
                for (ConsumerRecord<Long, GenericRecord> record : records) {
                    bytesRead.add(record.serializedValueSize());
                    cumulativeBytesRead.add(record.serializedValueSize());
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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
        return props;
    }
}
