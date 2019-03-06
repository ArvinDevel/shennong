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
import java.io.IOException;
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
import me.jinsui.shennong.bench.utils.CliFlags;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

/**
 * A perf reader to evaluate read performance of parquet over hdfs.
 */
@Slf4j
public class HDFSReader extends ReaderBase {

    /**
     * Flags for the read command.
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
            description = "Tpch table name, using tpch table schema when this specified.")
        public String tableName = null;

    }

    protected final Flags flags;

    public HDFSReader(Flags flags) {
        this.flags = flags;
    }

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running parquet reader over hdfs", e);
        }
    }

    protected void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting parquet reader over hdfs with config : {}", w.writeValueAsString(flags));

        // create dfs
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", flags.url);
        FileSystem dfs = FileSystem.get(configuration);
        List<Pair<Integer, ParquetReader<GenericRecord>>> streams = new ArrayList<>(flags.numFiles);
        for (int i = 0; i < flags.numFiles; i++) {
            Path path = new Path(flags.directory + String.format(flags.fileName, i));
            if (!dfs.exists(path)) {
                log.error("The path {} doesn't exists, existing now", path);
                System.exit(-1);
            }
            dfs.open(path);

            Schema writeSchema = User.getClassSchema();
            if (null != flags.tableName) {
                switch (flags.tableName) {
                    case "orders":
                        writeSchema = Orders.getClassSchema();
                        break;
                    case "lineitem":
                        writeSchema = Lineitem.getClassSchema();
                        break;
                    case "customer":
                        writeSchema = Customer.getClassSchema();
                        break;
                    case "part":
                        writeSchema = Part.getClassSchema();
                        break;
                    case "partsupp":
                        writeSchema = Partsupp.getClassSchema();
                        break;
                    case "supplier":
                        writeSchema = Supplier.getClassSchema();
                        break;
                }
            }
            String[] cols = flags.readColumn.split(",");
            ArrayList<Schema.Field> readFields = new ArrayList<>();
            List<Schema.Field> writeFields = writeSchema.getFields();

            boolean found = false;
            for (String col : cols) {
                for (Schema.Field field : writeFields) {
                    if (col.equals(field.name())) {
                        found = true;
                        Schema.Field fieldClone = new Schema.Field(col, field.schema(), field.doc(), field.defaultValue());
                        readFields.add(fieldClone);
                        break;
                    }
                }
                if (!found) {
                    log.error("Read col {} is not matched to writer schema {}", col, writeSchema);
                    System.exit(-1);
                }
            }

            Schema projectedSchema = Schema.createRecord(writeSchema.getName(), writeSchema.getDoc(), writeSchema.getNamespace(), writeSchema.isError());
            projectedSchema.setFields(readFields);
            log.info("read fields are {}", readFields);
            configuration.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, projectedSchema.toString());
            InputFile inputFile = HadoopInputFile.fromPath(path, configuration);
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .build();
            streams.add(Pair.of(i, reader));
        }
        execute(streams);
    }

    private void execute(List<Pair<Integer, ParquetReader<GenericRecord>>> streams) throws Exception {
        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<ParquetReader<GenericRecord>> logsThisThread = streams
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        read(logsThisThread);
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
            streams.forEach(streamPair -> {
                    try {
                        streamPair.getRight().close();
                    } catch (IOException ioe) {
                    }
                }
            );
        }
    }

    // read used by specific thread
    void read(List<ParquetReader<GenericRecord>> readers) throws Exception {
        log.info("Read thread started with : logs = {}",
            readers.stream().map(stream -> stream.toString()).collect(Collectors.toList()));

        final int numLogs = readers.size();
        GenericRecord readData;
        int backoffNum = 0;
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                readData = readers.get(i).read();
                if (null != readData) {
                    final long receiveTime = System.currentTimeMillis();
                    eventsRead.increment();
                    cumulativeEventsRead.increment();
                    // todo estimate read size
//                    bytesRead.add(readData.getEstimatedSize());
//                    cumulativeBytesRead.add(readEvents.getEstimatedSize());
                    // reset backoffNum
                    backoffNum = 0;
                } else if (flags.readEndless == 0) {
                    if (backoffNum > flags.maxBackoffNum) {
                        log.info("No more data after {} retry number, shut down", flags.maxBackoffNum);
                        System.exit(-1);
                    } else {
                        backoffNum++;
                    }
                }
            }
        }
    }

    private static Schema.Field findField(Schema schema, String name) {
        if (schema.getField(name) != null) {
            return schema.getField(name);
        }

        Schema.Field foundField = null;

        for (Schema.Field field : schema.getFields()) {
            Schema fieldSchema = field.schema();
            if (Schema.Type.RECORD.equals(fieldSchema.getType())) {
                foundField = findField(fieldSchema, name);
            } else if (Schema.Type.ARRAY.equals(fieldSchema.getType())) {
                foundField = findField(fieldSchema.getElementType(), name);
            } else if (Schema.Type.MAP.equals(fieldSchema.getType())) {
                foundField = findField(fieldSchema.getValueType(), name);
            }

            if (foundField != null) {
                return foundField;
            }
        }

        return foundField;
    }
}
