package Storm.SQSHandler;

import Storm.AMQPHandler.AMQPSpout;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SQSSpout implements IRichSpout {
    private static final Logger log = LoggerFactory.getLogger(AMQPSpout.class);
    private final Integer MESSAGE_POLL = 10;


    private SpoutOutputCollector _collector;
    private TopologyContext context;
    private String queue;
    private AmazonSQSAsync sqs;
    private String stream;


    private LinkedBlockingQueue<Message> localQueue;

    public SQSSpout(String queue, String stream) {
        this.queue = queue;
        this.stream = stream;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this._collector = spoutOutputCollector;
        localQueue = new LinkedBlockingQueue<>();
        try {
            // Todo: AWS Credentials
            sqs = new AmazonSQSAsyncClient(new PropertiesCredentials(new File("ss")));
        } catch (IOException e) {
            throw new RuntimeException("No AWS Credentials supplied! Failed to initialize SQS Spout.", e);
        }

    }

    @Override
    public void nextTuple() {
        if (queue.isEmpty()) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(new ReceiveMessageRequest(queue).withMaxNumberOfMessages(MESSAGE_POLL));
            localQueue.addAll(receiveMessageResult.getMessages());
        }
        Message message = localQueue.poll();
        if (message != null) {
            List<Object> toEmit = new ArrayList<>();
            toEmit.add(message);
            _collector.emit(stream, toEmit, message.getReceiptHandle());
        }


    }

    @Override
    public void ack(Object receiptHandle) {
        try {
            sqs.deleteMessageAsync(new DeleteMessageRequest(queue, (String) receiptHandle));
        } catch (AmazonClientException ace) {
            log.error(String.format("Failed to delete message %s following ack.", (String) receiptHandle));
        }

    }

    @Override
    public void fail(Object o) {

    }


    @Override
    public void close() {
        sqs.shutdown();
        ((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();

    }

    @Override
    public void activate() {


    }

    @Override
    public void deactivate() {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
