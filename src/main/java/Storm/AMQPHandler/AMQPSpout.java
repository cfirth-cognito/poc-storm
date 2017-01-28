package Storm.AMQPHandler;

import com.rabbitmq.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by Charlie on 28/01/2017.
 */
public class AMQPSpout implements IRichSpout {
    private SpoutOutputCollector outputCollector;
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
        this.outputCollector = spoutOutputCollector;
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
        System.out.println("CF AMQPSpout: Queue Declared and awaiting messages..");


        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                Delivery delivery = new Delivery(envelope.getDeliveryTag(), body);
                System.out.println("CF received msg " + envelope.getDeliveryTag());
                deliveries.add(delivery);
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        channel.basicConsume(queue, false, consumer);

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        List<Object> output = new ArrayList<>();

        if (deliveries.size() > 1) {
            final Delivery delivery = deliveries.remove(deliveries.size() - 1);
            if (delivery == null) return;
            String msgBody = "";
            try {
                msgBody = new String(delivery.getBody(), "UTF-8");
                System.out.println("CF got msg " + msgBody);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            output.add(msgBody);
        } else {
            return;
        }
        outputCollector.emit(output);


    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }


    // Declares fields to output from spout
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("body"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    class Delivery {
        long deliveryTag;
        byte[] body;

        Delivery(long deliveryTag, byte[] body) {
            this.deliveryTag = deliveryTag;
            this.body = body;
        }

        long getDeliveryTag() {
            return deliveryTag;
        }

        byte[] getBody() {
            return body;
        }
    }
}
