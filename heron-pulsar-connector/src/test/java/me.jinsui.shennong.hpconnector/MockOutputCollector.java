package me.jinsui.shennong.hpconnector;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.tuple.Tuple;

import java.util.Collection;
import java.util.List;


public class MockOutputCollector implements IOutputCollector {

    private boolean acked = false;
    private boolean failed = false;
    private Throwable lastError = null;
    private Tuple ackedTuple = null;
    private int numTuplesAcked = 0;

    @Override
    public void reportError(Throwable error) {
        lastError = error;
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    }

    @Override
    public void ack(Tuple input) {
        acked = true;
        failed = false;
        ackedTuple = input;
        ++numTuplesAcked;
    }

    @Override
    public void fail(Tuple input) {
        failed = true;
        acked = false;
    }

    public boolean acked() {
        return acked;
    }

    public boolean failed() {
        return failed;
    }

    public Throwable getLastError() {
        return lastError;
    }

    public Tuple getAckedTuple() {
        return ackedTuple;
    }

    public int getNumTuplesAcked() {
        return numTuplesAcked;
    }

    public void reset() {
        acked = false;
        failed = false;
        lastError = null;
        ackedTuple = null;
        numTuplesAcked = 0;
    }

}
