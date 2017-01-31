package Storm;

import Storm.AMQPHandler.AMQPSpout;
import Storm.AMQPHandler.ParseAMQPBolt;
import Storm.DatabaseHandler.DBObjects.Item;
import Storm.DatabaseHandler.DBObjects.ItemState;
import Storm.DatabaseHandler.InsertBolts.InsertBoltImpl;
import Storm.DatabaseHandler.ItemStateTransformBolt;
import Storm.DatabaseHandler.ItemTransformBolt;
import Storm.ErrorHandler.ErrorBolt;
import com.google.common.collect.Lists;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */

public class StormBase {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        Map<String, Object> configMap = new HashMap<>();
        JdbcMapper itemJdbcMapper = new SimpleJdbcMapper(Item.getColumns());
        JdbcMapper itemStateJdbcMapper = new SimpleJdbcMapper(ItemState.getColumns());

        configMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("dataSource.url", "jdbc:mysql://localhost:3306/hermes_mi");
        configMap.put("dataSource.user", "root");
        configMap.put("dataSource.password", "root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(configMap);

        System.out.println("Starting Storm..");

        // AMQP Spout - (add additional spouts for other msgtypes)
        AMQPSpout amqpSpout = new AMQPSpout("localhost",
                5672, "/", "guest", "guest", "mi-item-created");

        ParseAMQPBolt parseAMQPBolt = new ParseAMQPBolt();
        ItemTransformBolt itemTransformBolt = new ItemTransformBolt();
        ItemStateTransformBolt itemStateTransformBolt = new ItemStateTransformBolt();
        JdbcInsertBolt itemPersistenceBolt = new JdbcInsertBolt(connectionProvider, itemJdbcMapper)
                .withInsertQuery("insert into inv_item_d (" + Item.columnsToString() + ") values (" + Item.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);
        InsertBoltImpl itemStatePersistenceBolt = new InsertBoltImpl(connectionProvider, itemStateJdbcMapper)
                .withInsertQuery("insert into inv_item_state_f (" + ItemState.columnsToString() + ") values (" + ItemState.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);


        ErrorBolt errorBolt = new ErrorBolt();

        builder.setSpout("AMQPSpout", amqpSpout);
        builder.setBolt("parse_amqp_bolt", parseAMQPBolt)
                .shuffleGrouping("AMQPSpout", "item");
        builder.setBolt("item_transform_bolt", itemTransformBolt)
                .shuffleGrouping("parse_amqp_bolt", "item");
        builder.setBolt("persist_bolt", itemPersistenceBolt)
                .shuffleGrouping("item_transform_bolt", "item");
        builder.setBolt("item_state_transform_bolt", itemStateTransformBolt)
                .shuffleGrouping("item_transform_bolt", "item");
        builder.setBolt("item_state_persistence_bolt", itemStatePersistenceBolt)
                .shuffleGrouping("item_state_transform_bolt", "item-state");




        /* Error Handling */
        /* Handle inserting errors to PDI_LOGGING, and failing the tuple gracefully */
        /* TODO: database to log to     */
        builder.setBolt("error_bolt", errorBolt)
                .shuffleGrouping("parse_amqp_bolt", "ErrorStream")
                .shuffleGrouping("item_transform_bolt", "ErrorStream")
                .shuffleGrouping("item_state_transform_bolt", "ErrorStream")
                .shuffleGrouping("item_state_persistence_bolt", "ErrorStream");

        System.out.println("[LOG] Topology configured. Creating now..");
        builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("AMQPStormPOC", configMap, builder.createTopology());


        System.out.println("[LOG] Sleeping");
        Thread.sleep(1000000);

        cluster.shutdown();
    }
}


