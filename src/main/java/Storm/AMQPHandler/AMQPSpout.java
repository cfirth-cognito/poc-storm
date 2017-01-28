package Storm.AMQPHandler;

import com.rabbitmq.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private transient QueueingConsumer consumer;


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
        } catch (IOException e) {
            e.printStackTrace();
            // reconnect
        }

    }

    private void setUpConnection() throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setVirtualHost(vhost);
        factory.setUsername(username);
        factory.setPassword(password);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queue, false, false, false, null);
        System.out.println("AMQPSpout: Queue Declared and awaiting messages..");


        consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, true, consumer);

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

        try {
            final QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            if (delivery == null) return;

            List<Object> output = new ArrayList<>();
            final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            output.add(delivery.getBody());

            outputCollector.emit("amqp_spout", output);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }


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
}
