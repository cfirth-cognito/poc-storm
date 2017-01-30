package Storm.AMQPHandler;

import com.rabbitmq.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by Charlie on 28/01/2017.
 */
public class AMQPSpout implements IRichSpout {
    private SpoutOutputCollector _collector;
    private TopologyContext context;


    private String host;
    private int port;
    private String vhost;
    private String username;
    private String password;

    private String queue;


    private transient boolean active = true;
    private transient Connection connection;
    private transient Channel channel;
    private transient Consumer consumer;
    private transient String consumerTag;

    private ArrayList<Delivery> deliveries = new ArrayList<>();


    public AMQPSpout(String host, int port, String vhost, String username, String password, String queue) {
        this.host = host;
        this.port = port;
        this.vhost = vhost;
        this.username = username;
        this.password = password;
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
                System.out.println("[LOG] received msg " + envelope.getDeliveryTag());
                deliveries.add(delivery);
//                channel.basicAck(envelope.getDeliveryTag(), false); // Acking here means no retry functionality.
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
            String type = "";
            long deliveryTag = delivery.getDeliveryTag();

            try {
                msgBody = new String(delivery.getBody(), "UTF-8");
                type = delivery.getType();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            emitValues.add(msgBody);

            _collector.emit("item", emitValues, deliveryTag);
        }
    }

    @Override
    public void ack(Object o) {
        final long deliveryTag = (long) o;
        if (channel != null) {
            try {
                System.out.println(String.format("[LOG] Acking message %d", deliveryTag));
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
        // add fault-tolerant retry logic in here (avoid infinite loop while still trying a message atleast once)
    }

    // Declares fields to output from spout
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Decalare Message Type specific streams:
//        outputFieldsDeclarer.declareStream("item-stream", new Fields("type", "body"));

        outputFieldsDeclarer.declareStream("item", new Fields("body"));
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
