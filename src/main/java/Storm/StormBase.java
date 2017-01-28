package Storm;

import Storm.AMQPHandler.AMQPSpout;
import Storm.AMQPHandler.ParseAMQPBolt;
import Storm.DatabaseHandler.ItemTransformBolt;
import com.google.common.collect.Lists;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.topology.TopologyBuilder;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */

public class StormBase {
    public static void main(String[] args) throws InterruptedException {

        Map<String, Object> configMap = new HashMap<>();

        configMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("dataSource.url", "jdbc:mysql://localhost:3306/hermes_mi");
        configMap.put("dataSource.user", "root");
        configMap.put("dataSource.password", "root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(configMap);

        System.out.println("Starting Storm..");

        //AMQP Spout
        AMQPSpout amqpSpout = new AMQPSpout("localhost",
                5672, "/", "guest", "guest", "mi-item-created");

        ParseAMQPBolt parseAMQPBolt = new ParseAMQPBolt();
        ItemTransformBolt itemTransformBolt = new ItemTransformBolt();

        List<Column> columns = Lists.newArrayList(
                new Column("inv_item_ref", Types.VARCHAR),
                new Column("inv_item_class", Types.VARCHAR),
                new Column("inv_item_subclass", Types.VARCHAR),
                new Column("inv_item_status", Types.VARCHAR),
                new Column("inv_item_class_display", Types.VARCHAR),
                new Column("inv_item_subclass_display", Types.VARCHAR),
                new Column("inv_item_status_display", Types.VARCHAR),
                new Column("barcode", Types.VARCHAR),
                new Column("stated_day", Types.VARCHAR),
                new Column("stated_time", Types.VARCHAR),
                new Column("client", Types.VARCHAR),
                new Column("customer_name", Types.VARCHAR),
                new Column("customer_address_1", Types.VARCHAR),
                new Column("version", Types.INTEGER),
                new Column("event_date", Types.VARCHAR),
                new Column("postcode", Types.VARCHAR),
                new Column("route_type", Types.VARCHAR));

        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columns);
        JdbcInsertBolt itemPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into inv_item_d (inv_item_ref, inv_item_class, inv_item_subclass, inv_item_status, " +
                        "inv_item_class_display, inv_item_subclass_display, inv_item_status_display, " +
                        " barcode, stated_day, stated_time, client, customer_name, customer_address_1," +
                        "version, event_date, postcode, route_type) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .withQueryTimeoutSecs(30);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("AMQPSpout", amqpSpout);
        builder.setBolt("parse_amqp_bolt", parseAMQPBolt)
                .shuffleGrouping("AMQPSpout");
        builder.setBolt("item_transform_bolt", itemTransformBolt)
                .shuffleGrouping("parse_amqp_bolt");
        builder.setBolt("persist_bolt", itemPersistanceBolt)
                .shuffleGrouping("item_transform_bolt");

        System.out.println("Topology configured. Creating now..");
        builder.createTopology();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("AMQPStormPOC", configMap, builder.createTopology());

        System.out.println("CF Sleeping");
        Thread.sleep(1000000);

        cluster.shutdown();

    }



        /*
        A bolt per lookup...or one bolt which does all the lookups manually????
        //        String lookUpQuery = "";
//        SimpleJdbcLookupMapper lookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns)
//        JdbcLookupBolt userNameLookupBolt = new JdbcLookupBolt(connectionProvider, selectSql, lookupMapper)
//                .withQueryTimeoutSecs(30);

         */

//        builder.setBolt("persistencebolt");

    // Requires custom implementaion of JdbcMapper listing columns to be used
//        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
//                .withTableName("user")
//                .withQueryTimeoutSecs(30);
}


