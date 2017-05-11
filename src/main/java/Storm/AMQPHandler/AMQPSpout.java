package Storm.AMQPHandler;

import Storm.Util.PropertiesHolder;
import Storm.Util.Streams;
import com.rabbitmq.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by Charlie on 28/01/2017.
 */
public class AMQPSpout implements IRichSpout {
    private static final Logger log = LoggerFactory.getLogger(AMQPSpout.class);

    private SpoutOutputCollector _collector;
    private TopologyContext context;


    private String host;
    private int port;
    private String vhost;
    private String username;
    private String password;
    private String streamId;

    private String queue;


    private transient boolean active = true;
    private transient Connection connection;
    private transient Channel channel;
    private transient Consumer consumer;
    private transient String consumerTag;

    private ArrayList<Delivery> deliveries = new ArrayList<>();


    public AMQPSpout(String host, int port, String vhost, String username, String password, String queue, String streamId) {
        this.host = host;
        this.port = port;
        this.vhost = vhost;
        this.username = username;
        this.password = password;
        this.queue = queue;
        this.streamId = streamId;
    }

    public AMQPSpout(String queue, String streamId) {
        this.host = PropertiesHolder.rabbitHost;
        this.port = PropertiesHolder.rabbitPort;
        this.vhost = PropertiesHolder.rabbitVHost;
        this.username = PropertiesHolder.rabbitUser;
        this.password = PropertiesHolder.rabbitPass;
        this.streamId = streamId;
        this.queue = queue;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this._collector = spoutOutputCollector;
        try {
            setUpConnection();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            // reconnect
            open(map, topologyContext, spoutOutputCollector);
        }

    }

    private void setUpConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setVirtualHost(vhost);
        factory.setUsername(username);
        factory.setPassword(password);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queue, true, false, false, null);
        System.out.println("[LOG] AMQPSpout: Queue Declared and awaiting messages..");


        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String type = envelope.getRoutingKey().substring(0, envelope.getRoutingKey().lastIndexOf("-"));
                Delivery delivery = new Delivery(envelope.getDeliveryTag(), body, type);
                deliveries.add(delivery);
            }
        };

        this.consumerTag = channel.basicConsume(queue, false, consumer);
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.basicCancel(consumerTag);
            }

            if (connection != null) {
                connection.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // Add pausing & un-pausing spout logic here.
    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        Values emitValues = new Values();
        if (deliveries.size() > 0) {
            final Delivery delivery = deliveries.remove(deliveries.size() - 1);
            if (delivery == null) return;
            String msgBody = "";
            long deliveryTag = delivery.getDeliveryTag();

            try {
                msgBody = new String(delivery.getBody(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            log.warn("Outputting " + streamId);
            emitValues.add(msgBody);
            _collector.emit(streamId, emitValues, deliveryTag);
        }
    }

    @Override
    public void ack(Object o) {
        final long deliveryTag = (long) o;
        if (channel != null) {
            try {
                log.debug(String.format("Acking message %d", deliveryTag));
                channel.basicAck(deliveryTag, false);
            } catch (IOException e) {
                System.out.println(String.format("[LOG] Failed to ack delivery tag %d", deliveryTag));
            } catch (ShutdownSignalException e) {
                System.out.println(String.format("[LOG] Failed to connect to AMQP. Failed to ack delivery tag %d", deliveryTag));
            }
        }
    }

    @Override
    public void fail(Object o) {
        try {
            channel.basicAck((Long) o, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // add fault-tolerant retry logic in here (avoid infinite loop while still retrying a message at least once)
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.ITEM.id(), new Fields("body"));
        outputFieldsDeclarer.declareStream(Streams.ITEM_STATE.id(), new Fields("body"));
        outputFieldsDeclarer.declareStream(Streams.DROP.id(), new Fields("body"));
        outputFieldsDeclarer.declareStream(Streams.DROP_STATE.id(), new Fields("body"));
        outputFieldsDeclarer.declareStream(Streams.LIST.id(), new Fields("body"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    class Delivery {
        long deliveryTag;
        byte[] body;
        String type;

        Delivery(long deliveryTag, byte[] body, String type) {
            this.deliveryTag = deliveryTag;
            this.body = body;
            this.type = type;
        }

        long getDeliveryTag() {
            return deliveryTag;
        }

        byte[] getBody() {
            return body;
        }

        String getType() {
            return type;
        }
    }
}
