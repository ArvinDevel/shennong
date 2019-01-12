package me.jinsui.shennong.hpconnector;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class PulsarSpoutTest {

    protected ConsumerConfiguration consumerConf;
    protected PulsarSpout spout;
    protected MockSpoutOutputCollector mockCollector;
    protected Producer producer;
    protected String methodName;


    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
        setup();
    }

    protected void setup() throws Exception {



        consumerConf = new ConsumerConfiguration();
        consumerConf.setSubscriptionType(SubscriptionType.Shared);
        spout = new PulsarSpout( new ClientConfiguration(), consumerConf);
        mockCollector = new MockSpoutOutputCollector();
        SpoutOutputCollector collector = new SpoutOutputCollector(mockCollector);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-spout-" + methodName);
        when(context.getThisTaskId()).thenReturn(0);
        spout.open(Maps.newHashMap(), context, collector);
        producer = PulsarClient.create(spout.localClusterUrl).createProducer(spout.topic);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        producer.close();
        spout.close();
    }

    @SuppressWarnings("serial")
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

    @Test
    public void testBasic() throws Exception {
        String msgContent = "hello world";
        producer.send(msgContent.getBytes());
        spout.nextTuple();
        Assert.assertTrue(mockCollector.emitted());
        Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
        spout.ack(mockCollector.getLastMessage());
    }

    @Test
    public void testRedeliverOnFail() throws Exception {
        String msgContent = "hello world";
        producer.send(msgContent.getBytes());
        spout.nextTuple();
        spout.fail(mockCollector.getLastMessage());
        mockCollector.reset();
        Thread.sleep(150);
        spout.nextTuple();
        Assert.assertTrue(mockCollector.emitted());
        Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
        spout.ack(mockCollector.getLastMessage());
    }

    @Test
    public void testNoRedeliverOnAck() throws Exception {
        String msgContent = "hello world";
        producer.send(msgContent.getBytes());
        spout.nextTuple();
        spout.ack(mockCollector.getLastMessage());
        mockCollector.reset();
        spout.nextTuple();
        Assert.assertFalse(mockCollector.emitted());
        Assert.assertNull(mockCollector.getTupleData());
    }

    @Test
    public void testLimitedRedeliveriesOnTimeout() throws Exception {
        String msgContent = "chuck norris";
        producer.send(msgContent.getBytes());

        long startTime = System.currentTimeMillis();
        while (startTime + pulsarSpoutConf.getFailedRetriesTimeout(TimeUnit.MILLISECONDS) > System
                .currentTimeMillis()) {
            mockCollector.reset();
            spout.nextTuple();
            Assert.assertTrue(mockCollector.emitted());
            Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
            spout.fail(mockCollector.getLastMessage());
            // wait to avoid backoff
            Thread.sleep(500);
        }
        spout.nextTuple();
        spout.fail(mockCollector.getLastMessage());
        mockCollector.reset();
        Thread.sleep(500);
        spout.nextTuple();
        Assert.assertFalse(mockCollector.emitted());
        Assert.assertNull(mockCollector.getTupleData());
    }

    @Test
    public void testLimitedRedeliveriesOnCount() throws Exception {
        String msgContent = "hello world";
        producer.send(msgContent.getBytes());

        spout.nextTuple();
        Assert.assertTrue(mockCollector.emitted());
        Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
        spout.fail(mockCollector.getLastMessage());

        mockCollector.reset();
        Thread.sleep(150);

        spout.nextTuple();
        Assert.assertTrue(mockCollector.emitted());
        Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
        spout.fail(mockCollector.getLastMessage());

        mockCollector.reset();
        Thread.sleep(300);

        spout.nextTuple();
        Assert.assertTrue(mockCollector.emitted());
        Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
        spout.fail(mockCollector.getLastMessage());

        mockCollector.reset();
        Thread.sleep(500);
        spout.nextTuple();
        Assert.assertFalse(mockCollector.emitted());
        Assert.assertNull(mockCollector.getTupleData());
    }

    @Test
    public void testBackoffOnRetry() throws Exception {
        String msgContent = "chuck norris";
        producer.send(msgContent.getBytes());
        spout.nextTuple();
        spout.fail(mockCollector.getLastMessage());
        mockCollector.reset();
        // due to backoff we should not get the message again immediately
        spout.nextTuple();
        Assert.assertFalse(mockCollector.emitted());
        Assert.assertNull(mockCollector.getTupleData());
        Thread.sleep(100);
        spout.nextTuple();
        Assert.assertTrue(mockCollector.emitted());
        Assert.assertTrue(msgContent.equals(mockCollector.getTupleData()));
        spout.ack(mockCollector.getLastMessage());
    }

    @Test
    public void testMessageDrop() throws Exception {
        String msgContent = "message to be dropped";
        producer.send(msgContent.getBytes());
        spout.nextTuple();
        Assert.assertFalse(mockCollector.emitted());
        Assert.assertNull(mockCollector.getTupleData());
    }

    @SuppressWarnings({ "rawtypes" })
    @Test
    public void testMetrics() throws Exception {
        spout.resetMetrics();
        String msgContent = "hello world";
        producer.send(msgContent.getBytes());
        spout.nextTuple();
        Map metrics = spout.getMetrics();
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_RECEIVED)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_EMITTED)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_FAILED_MESSAGES)).longValue(), 0);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_ACKS)).longValue(), 1);
        Assert.assertEquals(((Double) metrics.get(PulsarSpout.CONSUMER_RATE)).doubleValue(),
                1.0 / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        Assert.assertEquals(((Double) metrics.get(PulsarSpout.CONSUMER_THROUGHPUT_BYTES)).doubleValue(),
                ((double) msgContent.getBytes().length) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        spout.fail(mockCollector.getLastMessage());
        metrics = spout.getMetrics();
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_RECEIVED)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_EMITTED)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_FAILED_MESSAGES)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_ACKS)).longValue(), 0);
        Thread.sleep(150);
        spout.nextTuple();
        metrics = spout.getMetrics();
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_RECEIVED)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_EMITTED)).longValue(), 2);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_FAILED_MESSAGES)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_ACKS)).longValue(), 1);
        spout.ack(mockCollector.getLastMessage());
        metrics = (Map) spout.getValueAndReset();
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_RECEIVED)).longValue(), 1);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_MESSAGES_EMITTED)).longValue(), 2);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_FAILED_MESSAGES)).longValue(), 0);
        Assert.assertEquals(((Long) metrics.get(PulsarSpout.NO_OF_PENDING_ACKS)).longValue(), 0);
    }



}
