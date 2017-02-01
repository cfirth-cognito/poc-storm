package Storm;

import Storm.AMQPHandler.AMQPSpout;
import Storm.AMQPHandler.ParseAMQPBolt;
import Storm.DatabaseHandler.DBObjects.Item;
import Storm.DatabaseHandler.DBObjects.ItemState;
import Storm.DatabaseHandler.InsertBolts.InsertBoltImpl;
import Storm.DatabaseHandler.ItemStateTransformBolt;
import Storm.DatabaseHandler.ItemTransformBolt;
import Storm.ErrorHandler.ErrorBolt;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */

public class StormBase {
    private static ConnectionProvider connectionProvider;

    /* Item Topology */
    private static AMQPSpout itemAMQPSpout = new AMQPSpout("localhost",
            5672, "/", "guest", "guest", "mi-item-created", "item");
    private static ItemTransformBolt itemTransformBolt = new ItemTransformBolt();
    private static JdbcMapper itemJdbcMapper = new SimpleJdbcMapper(Item.getColumns());
    private static InsertBoltImpl itemPersistenceBolt;

    /* Item State Topology */
    private static AMQPSpout itemStateAMQPSpout = new AMQPSpout("localhost",
            5672, "/", "guest", "guest", "mi-item-state-created", "item-state");
    private static ItemStateTransformBolt itemStateTransformBolt = new ItemStateTransformBolt();
    private static JdbcMapper itemStateJdbcMapper = new SimpleJdbcMapper(ItemState.getColumns());
    private static InsertBoltImpl itemStatePersistenceBolt;

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("dataSource.url", "jdbc:mysql://localhost:3306/hermes_mi");
        configMap.put("dataSource.user", "root");
        configMap.put("dataSource.password", "root");
        connectionProvider = new HikariCPConnectionProvider(configMap);

        System.out.println("Starting Storm..");

        /* Generic Processing Bolts */

        ParseAMQPBolt parseAMQPBolt = new ParseAMQPBolt();
        builder.setBolt("parse_amqp_bolt", parseAMQPBolt)
                .shuffleGrouping("ItemAMQPSpout", "item")
                .shuffleGrouping("ItemStateAMQPSpout", "item-state");

        /* Build Topology */

        builder = buildItemTopology(builder);
        builder = buildItemStateTopology(builder);
        builder = buildErrorTopology(builder);

        System.out.println("[LOG] Topology configured. Creating now..");
        builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("AMQPStormPOC", configMap, builder.createTopology());

        System.out.println("[LOG] Sleeping");
        Thread.sleep(1000000);
        cluster.shutdown();
    }

    private static TopologyBuilder buildItemTopology(TopologyBuilder builder) {
        itemPersistenceBolt = new InsertBoltImpl(connectionProvider, itemJdbcMapper)
                .withInsertQuery("insert into inv_item_d (" + Item.columnsToString() + ") values (" + Item.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout("ItemAMQPSpout", itemAMQPSpout);
        builder.setBolt("item_transform_bolt", itemTransformBolt)
                .shuffleGrouping("parse_amqp_bolt", "item");
        builder.setBolt("persist_bolt", itemPersistenceBolt)
                .shuffleGrouping("item_transform_bolt", "item");

//        builder.setBolt("item_state_transform_bolt", itemStateTransformBolt)

        return builder;
    }

    private static TopologyBuilder buildItemStateTopology(TopologyBuilder builder) {
        itemStatePersistenceBolt = new InsertBoltImpl(connectionProvider, itemStateJdbcMapper)
                .withInsertQuery("insert into inv_item_state_f (" + ItemState.columnsToString() + ") values (" + ItemState.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout("ItemStateAMQPSpout", itemStateAMQPSpout);
        builder.setBolt("item_state_transform_bolt", itemStateTransformBolt)
                .shuffleGrouping("persist_bolt", "item") // Item Created State
                .shuffleGrouping("parse_amqp_bolt", "item-state");
        builder.setBolt("item_state_persistence_bolt", itemStatePersistenceBolt)
                .shuffleGrouping("item_state_transform_bolt", "item-state");
        return builder;
    }

    /* Error Handling */
    /* Handle inserting errors to PDI_LOGGING, and failing the tuple gracefully */
    /* TODO: database to log to     */
    private static TopologyBuilder buildErrorTopology(TopologyBuilder builder) {
        ErrorBolt errorBolt = new ErrorBolt();
        builder.setBolt("error_bolt", errorBolt)
                .shuffleGrouping("parse_amqp_bolt", "ErrorStream")
                .shuffleGrouping("item_transform_bolt", "ErrorStream")
                .shuffleGrouping("item_state_transform_bolt", "ErrorStream")
                .shuffleGrouping("item_state_persistence_bolt", "ErrorStream");

        return builder;
    }

    /**
     * output tuple to internal spout
     * tuple includes inserted ID + other data
     *
     */
}


