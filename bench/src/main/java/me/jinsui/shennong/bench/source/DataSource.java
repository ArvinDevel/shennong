package me.jinsui.shennong.bench.source;

public interface DataSource<T> {
    boolean hasNext();

    T getNext();

    int getEventSize();
}
