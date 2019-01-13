package me.jinsui.shennong.bench.source;

import com.google.common.util.concurrent.RateLimiter;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class AvroDataSource implements DataSource<GenericRecord> {
    private final RateLimiter rateLimiter;
    private final String schemaFile;
    private final String mockStr = "mockName";
    private final int msgSize;
    private long uid = 0;

    public AvroDataSource(double rate, String schemaFile) {
        this.rateLimiter = RateLimiter.create(rate);
        this.schemaFile = schemaFile;
        this.msgSize = estimateMsgSize();
    }

    private int estimateMsgSize() {
        int estimatedSize = 0;
        for (Schema.Field field : User.getClassSchema().getFields()) {
            switch (field.schema().getType()) {
                case INT:
                    estimatedSize += 4;
                    break;
                case LONG:
                    estimatedSize += 8;
                    break;
                case BOOLEAN:
                    estimatedSize += 1;
                    break;
                case FLOAT:
                    estimatedSize += 4;
                    break;
                case DOUBLE:
                    estimatedSize += 8;
                    break;
                case BYTES:
                    estimatedSize += 8;
                    break;
                case STRING:
                    estimatedSize += mockStr.getBytes().length;
                    break;
                default:
                    log.error("{} is Not suitable type to estimate size", field.schema().getType());
            }
        }
        return estimatedSize;
    }

    @Override
    public boolean hasNext() {
        return rateLimiter.tryAcquire(msgSize);
    }

    public GenericRecord getNext() {
        ByteBuffer mockBytes = ByteBuffer.allocate(8);
        mockBytes.putLong(ThreadLocalRandom.current().nextLong());
        mockBytes.flip();
        return User.newBuilder()
            .setName(mockStr)
            .setAddress(mockStr)
            .setPhone(mockStr)
            .setToken(mockBytes)
            .setAge(ThreadLocalRandom.current().nextInt())
            .setWeight(ThreadLocalRandom.current().nextFloat())
            .setCtime(System.currentTimeMillis())
            .build();
    }

    @Override
    public int getEventSize() {
        return msgSize;
    }
}
