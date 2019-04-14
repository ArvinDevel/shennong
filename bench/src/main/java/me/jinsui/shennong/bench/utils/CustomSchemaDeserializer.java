package me.jinsui.shennong.bench.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import me.jinsui.shennong.bench.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomSchemaDeserializer<T extends GenericRecord> implements Deserializer<T> {

    Schema schema;
    private final String schemaStorePath = "schemaForKafka";

    public CustomSchemaDeserializer() {
        String schemaPath;
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(schemaStorePath));
            schemaPath = new String(encoded);
            schema = new Schema.Parser().parse(new File(schemaPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            DatumReader<T> userDatumReader = new SpecificDatumReader<>(schema);
            BinaryDecoder binaryEncoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);
            return userDatumReader.read(null, binaryEncoder);
        } catch (IOException ioe) {
            throw new SerializationException(ioe.getMessage());
        }
    }

    @Override
    public void close() {

    }
}
