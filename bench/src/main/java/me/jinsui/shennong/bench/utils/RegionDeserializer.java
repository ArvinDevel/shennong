package me.jinsui.shennong.bench.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import me.jinsui.shennong.bench.avro.Region;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class RegionDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            DatumReader<T> userDatumReader = new SpecificDatumReader<>(Region.getClassSchema());
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
