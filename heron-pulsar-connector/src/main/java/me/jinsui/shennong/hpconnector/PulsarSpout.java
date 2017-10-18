package me.jinsui.shennong.hpconnector;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.twitter.heron.api.metric.IMetric;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;
import com.yahoo.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by yaoguangzhong on 2017/8/17.
 * pull data from Pulsar use Pulsar's consumer api
 *
 * using steps:
 * 1. start local pulsar cluster
 * 2. run java test
 * ?3. start local heron cluster & submit topology
 */
public class PulsarSpout extends BaseRichSpout implements IMetric {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSpout.class);
    public static final int DEFAULT_METRICS_TIME_INTERVAL_IN_SECS = 60;

    public static final String NO_OF_PENDING_FAILED_MESSAGES = "numberOfPendingFailedMessages";
    public static final String NO_OF_MESSAGES_RECEIVED = "numberOfMessagesReceived";
    public static final String NO_OF_MESSAGES_EMITTED = "numberOfMessagesEmitted";
    public static final String NO_OF_PENDING_ACKS = "numberOfPendingAcks";
    public static final String CONSUMER_RATE = "consumerRate";
    public static final String CONSUMER_THROUGHPUT_BYTES = "consumerThroughput";

    private int metricsTimeIntervalInSecs = DEFAULT_METRICS_TIME_INTERVAL_IN_SECS;
    private volatile long messagesReceived = 0;
    private volatile long messagesEmitted = 0;
    private volatile long pendingAcks = 0;
    private volatile long messageSizeReceived = 0;
    private final ConcurrentMap<MessageId, MessageRetries> pendingMessageRetries = Maps.newConcurrentMap();
    private final Queue<Message> failedMessages = Queues.newConcurrentLinkedQueue();
    private final ConcurrentMap<String, Object> metricsMap = Maps.newConcurrentMap();


    private final ClientConfiguration clientConf;
    private final ConsumerConfiguration consumerConf;
    private Consumer consumer;
    private PulsarClient pulsarClient;

    private String componentId;
    private String spoutId;
    private SpoutOutputCollector collector;

    public String localClusterUrl = "pulsar://localhost:6650";
    public String namespace = "sample/standalone/ns1"; // This namespace is created automatically
    public String topic = String.format("persistent://%s/my-topic", namespace);
    public String subscription = "subcription-spout";


    public PulsarSpout(ClientConfiguration clientConf, ConsumerConfiguration consumerConf){
        this.clientConf = clientConf;
        this.consumerConf = consumerConf;
    }
    /**
     * create a Pulsar consumer and register the metrics to heron ctx
     */
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.componentId = topologyContext.getThisComponentId();
        this.spoutId = String.format("%s-%s",componentId,topologyContext.getThisTaskId());
        this.collector = collector;

        try{
            pulsarClient = PulsarClient.create(localClusterUrl);
            consumer = pulsarClient.subscribe(topic,subscription,consumerConf);

        }catch (PulsarClientException pce){
            LOG.error("[{}] Error creating pulsar client or consumer to pulsar cluster {}",spoutId,localClusterUrl,pce);
        }
        topologyContext.registerMetric(String.format("PulsarSpoutMetrics-%s", spoutId), this,
                metricsTimeIntervalInSecs);

    }

    /**
     * Emits a tuple received from the Pulsar consumer unless there are any failed messages
     */
    @Override
    public void nextTuple() {
        Message msg;
        if(consumer != null){
            if(LOG.isDebugEnabled()){
                LOG.debug("[{}] Receiving the next message from pulsar consumer to emit to the collector", spoutId);
            }
        }
        //todo deal failedMessages use failedMessages

        try{
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if(msg != null){
                ++messagesReceived;
                messageSizeReceived += msg.getData().length;
            }
            mapToValueAndEmit(msg);
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error receiving message from pulsar consumer", spoutId, e);
        }
    }
    @Override
    public void ack(Object msgId){
        if (msgId instanceof Message) {
            Message msg = (Message) msgId;
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Received ack for message {}", spoutId, msg.getMessageId());
            }
            consumer.acknowledgeAsync(msg);
            pendingMessageRetries.remove(msg.getMessageId());
            --pendingAcks;
        }
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        messageToValuesMapper.declareOutputFields(declarer);

    }


    private void mapToValueAndEmit(Message msg) {
        if (msg != null) {
            Values values = messageToValuesMapper.toValues(msg);
            ++pendingAcks;
            if (values == null) {
                // since the mapper returned null, we can drop the message and ack it immediately
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Dropping message {}", spoutId, msg.getMessageId());
                }
                ack(msg);
            } else {
                collector.emit(values, msg);
                ++messagesEmitted;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Emitted message {} to the collector", spoutId, msg.getMessageId());
                }
            }
        }
    }

    /**
     * Helpers for metrics
     */

    @SuppressWarnings({ "rawtypes" })
    ConcurrentMap getMetrics() {
        metricsMap.put(NO_OF_PENDING_FAILED_MESSAGES, (long) pendingMessageRetries.size());
        metricsMap.put(NO_OF_MESSAGES_RECEIVED, messagesReceived);
        metricsMap.put(NO_OF_MESSAGES_EMITTED, messagesEmitted);
        metricsMap.put(NO_OF_PENDING_ACKS, pendingAcks);
        metricsMap.put(CONSUMER_RATE, ((double) messagesReceived) / metricsTimeIntervalInSecs );
        metricsMap.put(CONSUMER_THROUGHPUT_BYTES,
                ((double) messageSizeReceived) / metricsTimeIntervalInSecs);
        return metricsMap;
    }

    void resetMetrics() {
        messagesReceived = 0;
        messagesEmitted = 0;
        messageSizeReceived = 0;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object getValueAndReset() {
        ConcurrentMap metrics = getMetrics();
        resetMetrics();
        return metrics;
    }
    public static MessageToValuesMapper messageToValuesMapper = new MessageToValuesMapper() {

        @Override
        public Values toValues(Message msg) {
            if ("message to be dropped".equals(new String(msg.getData()))) {
                return null;
            }
            return new Values(new String(msg.getData()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    };
    public class MessageRetries {
        private final long timestampInNano;
        private int numRetries;

        public MessageRetries() {
            this.timestampInNano = System.nanoTime();
            this.numRetries = 0;
        }

        public long getTimeStamp() {
            return timestampInNano;
        }

        public int incrementAndGet() {
            return ++numRetries;
        }

        public int getNumRetries() {
            return numRetries;
        }
    }




}
