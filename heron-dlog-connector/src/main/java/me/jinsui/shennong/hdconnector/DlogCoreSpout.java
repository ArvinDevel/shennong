package me.jinsui.shennong.hdconnector;

import com.twitter.heron.api.metric.IMetric;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import java.util.Map;

/**
 * Created by yaoguangzhong on 2017/8/21.
 * use dlog core lib to pull data from dlog
 */
public class DlogCoreSpout extends BaseRichSpout implements IMetric {
    public Object getValueAndReset() {
        return null;
    }

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    public void nextTuple() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
