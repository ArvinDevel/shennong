package me.jinsui.shennong.bench.writer;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.source.AvroDataSource;
import me.jinsui.shennong.bench.source.DataSource;
import me.jinsui.shennong.bench.source.TpchDataSourceFactory;
import me.jinsui.shennong.bench.utils.AvroSerializer;
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 * Write avro data to kafka cluster.
 */
@Slf4j
public class KafkaWriter extends WriterBase {

    /**
     * Flags for the write command.
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
                "-vt", "--value-type"
            },
            description = "Value type, default is 0, indicating avro, 1 indicatates byte[],")
        public int valueType = 0;

        @Parameter(
            names = {
                "-vs", "--value-size"
            },
            description = "Value size, used for byte[] size")
        public int valueSize = 100;

        @Parameter(
            names = {
                "-r", "--rate"
            },
            description = "Write rate bytes/s across all topic")
        public double writeRate = 1000000;

        @Parameter(
            names = {
                "-sf", "--schema-file"
            },
            description = "Schema represented as Avro, used in complex mode")
        public String schemaFile = null;

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
                "-ttn", "--tpch-table-name"
            },
            description = "Tpch table name, using tpch data when this specified.")
        public String tableName = null;

        @Parameter(
            names = {
                "-tsf", "--tpch-scale-factor"
            },
            description = "Tpch table generate data scale factor, default 1.")
        public int scaleFactor = 1;

    }

    private final Flags flags;
    private final KafkaProducer<Long, byte[]> bytesProducer;
    private final byte[] payload;


    public KafkaWriter(Flags flags) {
        this.flags = flags;
        if (flags.valueType > 0) {
            this.bytesProducer = new KafkaProducer<>(newBytesValueKafkaProperties(flags));
            Random random = new Random(0);
            if (flags.valueSize < 1) {
                log.error("Value size should larger than 0, use 10 bytes");
                flags.valueSize = 10;
            }
            payload = new byte[flags.valueSize];
            for (int i = 0; i < payload.length; ++i)
                payload[i] = (byte) (random.nextInt(26) + 65);

        } else {
            this.bytesProducer = null;
            this.payload = null;
        }
    }

    private Properties newKafkaProperties(Flags flags) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, flags.url);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        return props;
    }

    private Properties newBytesValueKafkaProperties(Flags flags) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, flags.url);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }

    @Override
    void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting kafka writer with config : {}", w.writeValueAsString(flags));

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
                final List<String> logsThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                final long numRecordsForThisThread = flags.numEvents / flags.numThreads;
                final long numBytesForThisThread = flags.numBytes / flags.numThreads;
                executor.submit(() -> {
                    try {
                        write(
                            logsThisThread,
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
        }
    }

    private void write(List<String> streams,
                       long numRecordsForThisThread,
                       long numBytesForThisThread) throws Exception {

        log.info("Write thread started with : logs = {},"
                + " num records = {}, num bytes = {}",
            streams.stream().map(l -> l).collect(Collectors.toList()),
            numRecordsForThisThread,
            numBytesForThisThread);
        final DataSource<GenericRecord> dataSource;
        if (null != flags.tableName) {
            dataSource = TpchDataSourceFactory.getTblDataSource(flags.writeRate, flags.tableName, flags.scaleFactor);
        } else {
            dataSource = new AvroDataSource(flags.writeRate, flags.schemaFile);
        }

        // one thread use one dedicated producer to avoid shared resource contention
        KafkaProducer<Long, GenericRecord> producer = new KafkaProducer<>(newKafkaProperties(flags));
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
                    try {
                        if (flags.valueType > 0) {
                            bytesProducer.send(new ProducerRecord<>(streams.get(i), System.currentTimeMillis(), payload),
                                (metadata, exception) -> {
                                    if (null != exception) {
                                        log.error("Write fail", exception);
                                        isDone.set(true);
                                        System.exit(-1);
                                    } else {
                                        eventsWritten.increment();
                                        bytesWritten.add(flags.valueSize);
                                        cumulativeEventsWritten.increment();
                                        cumulativeBytesWritten.add(flags.valueSize);

                                        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                                            System.nanoTime() - sendTime
                                        );
                                        recorder.recordValue(latencyMicros);
                                        cumulativeRecorder.recordValue(latencyMicros);
                                    }
                                });
                        } else {
                            GenericRecord msg = dataSource.getNext();
                            producer.send(new ProducerRecord<>(streams.get(i), System.currentTimeMillis(), msg),
                                (metadata, exception) -> {
                                    if (null != exception) {
                                        log.error("Write fail", exception);
                                        isDone.set(true);
                                        System.exit(-1);
                                    } else {
                                        eventsWritten.increment();
                                        bytesWritten.add(eventSize);
                                        cumulativeEventsWritten.increment();
                                        cumulativeBytesWritten.add(eventSize);

                                        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                                            System.nanoTime() - sendTime
                                        );
                                        recorder.recordValue(latencyMicros);
                                        cumulativeRecorder.recordValue(latencyMicros);
                                    }
                                });
                        }
                    } catch (final SerializationException se) {
                        log.error("Serialize msg fail ", se);
                        isDone.set(true);
                        System.exit(-1);
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

}
