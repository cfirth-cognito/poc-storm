package Storm.Util;

import Storm.AMQPHandler.JSONObjects.ItemState;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Charlie on 21/02/2017.
 */
public class SequencingBolt implements IRichBolt {

    TopologyContext context;
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this._collector = outputCollector;
    }

    /*
        Item Workflow: Transform item. Persist. Check for Item States from Cassandra related to Item. Emit state messages to Item-State stream.
        Item State Workflow: Transform Item-State. Check DB for item; either continue or fail tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        Cluster cassandraCluster = Cluster.builder()
                .addContactPoints(PropertiesHolder.getValue("cassandra_urls").split(","))
                .withClusterName("foo")
                .withPort(9042)
                .withCredentials("guest", "guest")
                .build();
        Session session = cassandraCluster.connect();
        String query;
        ResultSet resultSet;

        switch (tuple.getSourceStreamId()) {
            case "item":
                /* Goto Cassandra. Check for waiting item states. Emit the messages into the usual Item State transformation */
                Tuple item = (Tuple) tuple.getValueByField("values");

                query = String.format("SELECT * FROM cqrs.event where aggregateid = '%s' and aggregatetype = 'item';",
                        item.getStringByField("inv_item_ref"));
                resultSet = session.execute(query);

                for (Row state : resultSet) {
                    if (state.getString("eventname").equalsIgnoreCase("transitioned"))
                        _collector.emit("item-state", tuple, new Values(state.getMap("parameters", String.class, String.class).get("state")));
                }

                break;
            case "item-state":
                /* Check for Item */
                ItemState itemState = (ItemState) tuple.getValueByField("item-state");

                query = String.format("SELECT * FROM cqrs.event where aggregateid = '%s' and aggregatetype = 'item';",
                        itemState.getReference());
                resultSet = session.execute(query);

                boolean found = false;
                for (Row state : resultSet) {
                    if (state.getString("eventname").equalsIgnoreCase("created")) {
                        _collector.emit("item-state", tuple, new Values(itemState));
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
        outputFieldsDeclarer.declareStream("item-state", new Fields("item-state"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}