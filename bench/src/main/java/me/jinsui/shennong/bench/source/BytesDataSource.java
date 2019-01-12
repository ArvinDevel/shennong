package me.jinsui.shennong.bench.source;

import com.google.common.util.concurrent.RateLimiter;

public class BytesDataSource implements DataSource<byte[]> {
    private RateLimiter rateLimiter;
    private final int msgSize;
    private final int rate;

    public BytesDataSource(int msgSize, int rate) {
        this.msgSize = msgSize;
        this.rateLimiter = RateLimiter.create(rate);
        this.rate = rate;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public byte[] getNext() {
        return new byte[0];
    }

    @Override
    public int getEventSize() {
        return msgSize;
    }
}
