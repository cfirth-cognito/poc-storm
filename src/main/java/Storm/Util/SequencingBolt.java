package Storm.Util;

import Storm.AMQPHandler.JSONObjects.ItemState;
import Storm.AMQPHandler.Parser;
import Storm.DatabaseHandler.LookupHandler;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Charlie on 21/02/2017.
 */
public class SequencingBolt implements IRichBolt {
    private static final Logger log = LoggerFactory.getLogger(SequencingBolt.class);

    TopologyContext context;
    OutputCollector _collector;

    private Session session;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;
    }

    private void getSession() {
        Cluster cassandraCluster = Cluster.builder()
                .addContactPoints(PropertiesHolder.getValue("cassandra.urls").split(","))
                .withClusterName("foo")
                .withPort(9042)
                .withCredentials("guest", "guest")
                .build();
        if (session == null || session.isClosed())
            session = cassandraCluster.connect();
    }

    /*
        Item Workflow: Transform item. Persist. Check for Item States from Cassandra related to Item. Emit state messages to Item-State stream.
        Item State Workflow: Transform Item-State. Check DB for item; either continue or fail tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        getSession();
        String query;
        ResultSet resultSet;

        switch (tuple.getSourceStreamId()) {
            case "item":
                /* Goto Cassandra. Check for waiting item states. Emit the messages into the usual Item State transformation */
                Tuple item = (Tuple) tuple.getValueByField("values");

                query = String.format("SELECT * FROM cqrs.event where aggregateid = '%s' and aggregatetype = 'item';",
                        item.getStringByField("inv_item_ref"));
                log.info("Checking for Item States for: " + item.getStringByField("inv_item_ref"));
                resultSet = session.execute(query);
                for (Row state : resultSet) {
                    String eventName = state.getString("eventname");
                    log.info("Processing result with event name of " + eventName);
                    if (eventName.equalsIgnoreCase("transitioned"))
                        _collector.emit("item-state", tuple, new Values(state.getMap("parameters", String.class, String.class).get("state")));
                }

                break;
            case "item-state":
                /* Check for Item */
                ItemState itemState = (ItemState) tuple.getValueByField("item-state");

                log.info("Checking for existing item before processing Item State");

                query = String.format("SELECT * FROM cqrs.event where aggregateid = '%s' and aggregatetype = 'item';",
                        itemState.getReference());
                resultSet = session.execute(query);

                boolean found = false;
                for (Row state : resultSet) {
                    if (state.getString("eventname").equalsIgnoreCase("created")) {
                        log.info("Found item. Continuing Item State stream.");
                        _collector.emit("item-state-cont", tuple, new Values(itemState));
                        found = true;
                        break;
                    }
                }

                if (!found)
                    _collector.ack(tuple); // No item yet. Don't go any further, and remove the state from Rabbit
                break;
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.ITEM_STATE.id(), new Fields("item-state"));
        outputFieldsDeclarer.declareStream("item-state-cont", new Fields("item-state"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
