package Storm;

import Storm.AMQPHandler.AMQPSpout;
import Storm.AMQPHandler.ParseAMQPBolt;
import Storm.DatabaseHandler.DBObjects.Item;
import Storm.DatabaseHandler.DBObjects.ItemState;
import Storm.DatabaseHandler.DBObjects.List;
import Storm.DatabaseHandler.InsertBolts.InsertBoltImpl;
import Storm.Transformers.ItemStateTransformBolt;
import Storm.Transformers.ItemTransformBolt;
import Storm.ErrorHandler.ErrorBolt;
import Storm.Util.PropertiesHolder;
import Storm.Util.SequencingBolt;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */

public class StormBase {
    private static final Logger log = LoggerFactory.getLogger(StormBase.class);
    private static ConnectionProvider connectionProvider;

    /* Item Topology */
    private static AMQPSpout itemAMQPSpout;
    private static ItemTransformBolt itemTransformBolt;
    private static JdbcMapper itemJdbcMapper;
    private static InsertBoltImpl itemPersistenceBolt;

    /* Item State Topology */
    private static AMQPSpout itemStateAMQPSpout;
    private static ItemStateTransformBolt itemStateTransformBolt;
    private static JdbcMapper itemStateJdbcMapper;
    private static InsertBoltImpl itemStatePersistenceBolt;

    private static SequencingBolt sequencingBolt;

    /* ListObj Topology */
//    private static AMQPSpout listAMQPSpout;
//    private static ItemTransformBolt listTransformBolt;
//    private static JdbcMapper listJdbcMapper;
//    private static InsertBoltImpl listPersistenceBolt;

    public static void main(String[] args) throws InterruptedException, IOException {
        TopologyBuilder builder = new TopologyBuilder();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("dataSource.url", String.format("jdbc:mysql://%s:%s/%s",
                PropertiesHolder.databaseHost, PropertiesHolder.databasePort, PropertiesHolder.databaseSchema));
        configMap.put("dataSource.user", PropertiesHolder.databaseUser);
        configMap.put("dataSource.password", PropertiesHolder.databasePass);
        connectionProvider = new HikariCPConnectionProvider(configMap);
        defineTasks();

        log.info("Starting Storm..");

        /* Generic Processing Bolts */

        ParseAMQPBolt parseAMQPBolt = new ParseAMQPBolt();
        builder.setBolt("parse_amqp_bolt", parseAMQPBolt)
                .shuffleGrouping("ItemAMQPSpout", "item")
                .shuffleGrouping("ItemStateAMQPSpout", "item-state")
                .shuffleGrouping("sequencing_bolt", "item-state");

//                .shuffleGrouping("ListAMQPSpout", "list");

        /* Build Topology */

        builder = buildItemTopology(builder);
        builder = buildItemStateTopology(builder);
        builder = buildErrorTopology(builder);

        log.info("Topology configured. Creating now..");
        builder.createTopology();
        String production = PropertiesHolder.production;

        System.out.println(production);
        if (production.equalsIgnoreCase("false")) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("AMQPStormPOC", configMap, builder.createTopology());

            System.out.println("[LOG] Sleeping");
            Thread.sleep(1000000);
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology("Storm-POC", configMap, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                log.error("Storm cluster already running!!");
                e.printStackTrace();
            }
        }

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
        builder.setBolt("sequencing_bolt", sequencingBolt)
                .shuffleGrouping("persist_bolt", "item")
                .shuffleGrouping("parse_amqp_bolt", "item-state");

        return builder;
    }

    private static TopologyBuilder buildItemStateTopology(TopologyBuilder builder) {
        itemStatePersistenceBolt = new InsertBoltImpl(connectionProvider, itemStateJdbcMapper)
                .withInsertQuery("insert into inv_item_state_f (" + ItemState.columnsToString() + ") values (" + ItemState.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout("ItemStateAMQPSpout", itemStateAMQPSpout);
        builder.setBolt("item_state_transform_bolt", itemStateTransformBolt)
                .shuffleGrouping("persist_bolt", "item") // Item Created State
                .shuffleGrouping("sequencing_bolt", "item-state-cont");
        builder.setBolt("item_state_persistence_bolt", itemStatePersistenceBolt)
                .shuffleGrouping("item_state_transform_bolt", "item-state");
        return builder;
    }

//    private static TopologyBuilder buildListTopology(TopologyBuilder builder) {
//        listPersistenceBolt = new InsertBoltImpl(connectionProvider, listJdbcMapper)
//                .withInsertQuery("insert into inv_list_d (" + List.columnsToString() + ") values (" + List.getPlaceholders() + ")")
//                .withQueryTimeoutSecs(30);
//
//        builder.setSpout("ListAMQPSpout", listAMQPSpout);
//        builder.setBolt("list_transform_bolt", listTransformBolt)
//                .shuffleGrouping("parse_amqp_bolt", "list");
//        builder.setBolt("persist_bolt", listPersistenceBolt)
//                .shuffleGrouping("list_transform_bolt", "list");
//
//        return builder;
//    }


    /* Error Handling */
    /* Handle inserting errors to PDI_LOGGING, and failing the tuple gracefully */
    /* TODO: database to log to     */
    private static TopologyBuilder buildErrorTopology(TopologyBuilder builder) {
        ErrorBolt errorBolt = new ErrorBolt();
        builder.setBolt("error_bolt", errorBolt)
                .shuffleGrouping("persist_bolt", "ErrorStream")
                .shuffleGrouping("parse_amqp_bolt", "ErrorStream")
                .shuffleGrouping("item_transform_bolt", "ErrorStream")
                .shuffleGrouping("item_state_transform_bolt", "ErrorStream")
                .shuffleGrouping("item_state_persistence_bolt", "ErrorStream");

        return builder;
    }

    private static void defineTasks() {
        sequencingBolt = new SequencingBolt();
        itemAMQPSpout = new AMQPSpout(PropertiesHolder.rabbitHost, PropertiesHolder.rabbitPort, PropertiesHolder.rabbitVHost,
                PropertiesHolder.rabbitUser, PropertiesHolder.rabbitPass, PropertiesHolder.itemQueue, "item");
        itemTransformBolt = new ItemTransformBolt();
        itemJdbcMapper = new SimpleJdbcMapper(Item.getColumns());

        itemStateAMQPSpout = new AMQPSpout(PropertiesHolder.rabbitHost, PropertiesHolder.rabbitPort, PropertiesHolder.rabbitVHost,
                PropertiesHolder.rabbitUser, PropertiesHolder.rabbitPass, PropertiesHolder.itemStateQueue, "item-state");
        itemStateTransformBolt = new ItemStateTransformBolt();
        itemStateJdbcMapper = new SimpleJdbcMapper(ItemState.getColumns());


//        listAMQPSpout = new AMQPSpout(PropertiesHolder.rabbitHost, PropertiesHolder.rabbitPort, PropertiesHolder.rabbitVHost,
//                PropertiesHolder.rabbitUser, PropertiesHolder.rabbitPass, PropertiesHolder.listQueue, "list");
//        itemTransformBolt = new ItemTransformBolt();
//        listJdbcMapper = new SimpleJdbcMapper(List.getColumns());
    }
}


