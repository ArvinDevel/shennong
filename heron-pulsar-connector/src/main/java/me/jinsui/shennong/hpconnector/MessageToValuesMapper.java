package me.jinsui.shennong.hpconnector;

import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Values;
import com.yahoo.pulsar.client.api.Message;

import java.io.Serializable;

public interface MessageToValuesMapper extends Serializable {

    /**
     * Convert {@link com.yahoo.pulsar.client.api.Message} to tuple values.
     *
     * @param msg
     * @return
     */
    public Values toValues(Message msg);

    /**
     * Declare the output schema for the spout.
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer);
}
