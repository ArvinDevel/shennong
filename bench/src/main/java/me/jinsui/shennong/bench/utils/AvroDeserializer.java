package me.jinsui.shennong.bench.utils;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.Schema.Parser;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            Schema schema = new Parser().parse(new File("me/jinsui/shennong/bench/avro/user.avsc"));
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
