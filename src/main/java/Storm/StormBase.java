package Storm;

import Storm.AMQPHandler.AMQPSpout;
import Storm.AMQPHandler.ParseAMQPBolt;
import Storm.DatabaseHandler.DBObjects.*;
import Storm.DatabaseHandler.InsertBolts.InsertBoltImpl;
import Storm.ErrorHandler.ErrorBolt;
import Storm.Transform.Bolts.*;
import Storm.Util.PropertiesHolder;
import Storm.Util.SequencingBolt;
import Storm.Util.Streams;
import Storm.Util.Topology;
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

    /* Drop Topology */
    private static AMQPSpout dropAMQPSpout;
    private static DropTransformBolt dropTransformBolt;
    private static JdbcMapper dropJdbcMapper;
    private static InsertBoltImpl dropPersistenceBolt;

    /* Drop State Topology */
    private static AMQPSpout dropStateAMQPSpout;
    private static DropStateTransformBolt dropStateTransformBolt;
    private static JdbcMapper dropStateJdbcMapper;
    private static InsertBoltImpl dropStatePersistenceBolt;

    /* ListObj Topology */
    private static AMQPSpout listAMQPSpout;
    private static ListTransformBolt listTransformBolt;
    private static JdbcMapper listJdbcMapper;
    private static InsertBoltImpl listPersistenceBolt;


    private static SequencingBolt sequencingBolt;


    public static void main(String[] args) throws InterruptedException, IOException {
        TopologyBuilder builder = new TopologyBuilder();
        Map<String, Object> configMap = initialize();

        log.info("Starting Storm..");

        /* Build Topology */

        builder = buildBaseTopology(builder);
        builder = buildItemTopology(builder);
        builder = buildItemStateTopology(builder);
        builder = buildDropTopology(builder);
        builder = buildDropStateTopology(builder);
        builder = buildListTopology(builder);
        builder = buildErrorTopology(builder);

        log.info("Topology configured. Creating now..");
        builder.createTopology();

        boolean production = Boolean.parseBoolean(PropertiesHolder.production);

        if (!production) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(Topology.TOPOLOGY_NAME.getId(), configMap, builder.createTopology());

            System.out.println("[LOG] Running in Local Mode. Sleeping....");
            Thread.sleep(1000000);
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology(Topology.TOPOLOGY_NAME.getId(), configMap, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                log.error("Storm cluster already running!");
                e.printStackTrace();
            }
        }

    }

    private static Map<String, Object> initialize() {
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("dataSource.url", String.format("jdbc:mysql://%s:%s/%s",
                PropertiesHolder.databaseHost, PropertiesHolder.databasePort, PropertiesHolder.databaseSchema));
        configMap.put("dataSource.user", PropertiesHolder.databaseUser);
        configMap.put("dataSource.password", PropertiesHolder.databasePass);
        connectionProvider = new HikariCPConnectionProvider(configMap);

        sequencingBolt = new SequencingBolt();

        itemAMQPSpout = new AMQPSpout(PropertiesHolder.itemQueue, Streams.ITEM.id());
        itemTransformBolt = new ItemTransformBolt();
        itemJdbcMapper = new SimpleJdbcMapper(Item.getColumns());

        itemStateAMQPSpout = new AMQPSpout(PropertiesHolder.itemStateQueue, Streams.ITEM_STATE.id());
        itemStateTransformBolt = new ItemStateTransformBolt();
        itemStateJdbcMapper = new SimpleJdbcMapper(ItemState.getColumns());


        dropAMQPSpout = new AMQPSpout(PropertiesHolder.dropQueue, Streams.DROP.id());
        dropTransformBolt = new DropTransformBolt();
        dropJdbcMapper = new SimpleJdbcMapper(Drop.getColumns());

        dropStateAMQPSpout = new AMQPSpout(PropertiesHolder.dropStateQueue, Streams.DROP_STATE.id());
        dropStateTransformBolt = new DropStateTransformBolt();
        dropStateJdbcMapper = new SimpleJdbcMapper(DropState.getColumns());


        listAMQPSpout = new AMQPSpout(PropertiesHolder.listQueue, Streams.LIST.id());
        listTransformBolt = new ListTransformBolt();
        listJdbcMapper = new SimpleJdbcMapper(ListObj.getColumns());

        return configMap;
    }


    /* Generic Processing Bolts */
    private static TopologyBuilder buildBaseTopology(TopologyBuilder builder) {
        ParseAMQPBolt parseAMQPBolt = new ParseAMQPBolt();
        builder.setBolt(Topology.PARSE_BOLT.getId(), parseAMQPBolt)
                .shuffleGrouping(Topology.ITEM_SPOUT.getId(), Streams.ITEM.id())
                .shuffleGrouping(Topology.ITEM_STATE_SPOUT.getId(), Streams.ITEM_STATE.id())
                .shuffleGrouping(Topology.DROP_SPOUT.getId(), Streams.DROP.id())
                .shuffleGrouping(Topology.DROP_STATE_SPOUT.getId(), Streams.DROP_STATE.id())
                .shuffleGrouping(Topology.LIST_SPOUT.getId(), Streams.LIST.id())
                .shuffleGrouping(Topology.SEQUENCING_BOLT.getId(), Streams.ITEM_STATE.id());

        builder.setBolt(Topology.SEQUENCING_BOLT.getId(), sequencingBolt)
                .shuffleGrouping(Topology.ITEM_PERSIST_BOLT.getId(), Streams.ITEM.id())
                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.ITEM_STATE.id())
//                .shuffleGrouping(Topology.DROP_PERSIST_BOLT.getId(), Streams.DROP.id())
                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.DROP_STATE.id());
        return builder;
    }

    private static TopologyBuilder buildItemTopology(TopologyBuilder builder) {
        itemPersistenceBolt = new InsertBoltImpl(connectionProvider, itemJdbcMapper)
                .withInsertQuery("insert into inv_item_d (" + Item.columnsToString() + ") values (" + Item.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout(Topology.ITEM_SPOUT.getId(), itemAMQPSpout);
        builder.setBolt(Topology.ITEM_TRANSFORM_BOLT.getId(), itemTransformBolt)
                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.ITEM.id());
        builder.setBolt(Topology.ITEM_PERSIST_BOLT.getId(), itemPersistenceBolt)
                .shuffleGrouping(Topology.ITEM_TRANSFORM_BOLT.getId(), Streams.ITEM.id());
//        builder.setBolt(Topology.SEQUENCING_BOLT.getId(), sequencingBolt)
//                .shuffleGrouping(Topology.ITEM_PERSIST_BOLT.getId(), Streams.ITEM.id())
//                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.ITEM_STATE.id());

        return builder;
    }

    private static TopologyBuilder buildItemStateTopology(TopologyBuilder builder) {
        itemStatePersistenceBolt = new InsertBoltImpl(connectionProvider, itemStateJdbcMapper)
                .withInsertQuery("insert into inv_item_state_f (" + ItemState.columnsToString() + ") values (" + ItemState.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout(Topology.ITEM_STATE_SPOUT.getId(), itemStateAMQPSpout);
        builder.setBolt(Topology.ITEM_STATE_TRANSFORM_BOLT.getId(), itemStateTransformBolt)
                .shuffleGrouping(Topology.ITEM_PERSIST_BOLT.getId(), Streams.ITEM.id()) // Item Created State
                .shuffleGrouping(Topology.SEQUENCING_BOLT.getId(), "item-state-cont");
        builder.setBolt(Topology.ITEM_STATE_PERSIST_BOLT.getId(), itemStatePersistenceBolt)
                .shuffleGrouping(Topology.ITEM_STATE_TRANSFORM_BOLT.getId(), Streams.ITEM_STATE.id());
        return builder;
    }

    private static TopologyBuilder buildDropTopology(TopologyBuilder builder) {
        dropPersistenceBolt = new InsertBoltImpl(connectionProvider, dropJdbcMapper)
                .withInsertQuery("insert into drop_d (" + Drop.columnsToString() + ") values (" + Drop.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout(Topology.DROP_SPOUT.getId(), dropAMQPSpout);
        builder.setBolt(Topology.DROP_TRANSFORM_BOLT.getId(), dropTransformBolt)
                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.DROP.id());
        builder.setBolt(Topology.DROP_PERSIST_BOLT.getId(), dropPersistenceBolt)
                .shuffleGrouping(Topology.DROP_TRANSFORM_BOLT.getId(), Streams.DROP.id());
//        builder.setBolt(Topology.SEQUENCING_BOLT.getId(), sequencingBolt)
//                .shuffleGrouping(Topology.DROP_PERSIST_BOLT.getId(), Streams.DROP.id())
//                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.DROP_STATE.id());

        return builder;
    }

    private static TopologyBuilder buildDropStateTopology(TopologyBuilder builder) {
        dropStatePersistenceBolt = new InsertBoltImpl(connectionProvider, dropStateJdbcMapper)
                .withInsertQuery("insert into drop_state_f (" + DropState.columnsToString() + ") values (" + DropState.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout(Topology.DROP_STATE_SPOUT.getId(), dropStateAMQPSpout);
        builder.setBolt(Topology.DROP_STATE_TRANSFORM_BOLT.getId(), dropStateTransformBolt)
                .shuffleGrouping(Topology.SEQUENCING_BOLT.getId(), "drop-state-cont");
        builder.setBolt(Topology.DROP_STATE_PERSIST_BOLT.getId(), dropStatePersistenceBolt)
                .shuffleGrouping(Topology.DROP_STATE_TRANSFORM_BOLT.getId(), Streams.DROP_STATE.id());
        return builder;
    }

    private static TopologyBuilder buildListTopology(TopologyBuilder builder) {
        listPersistenceBolt = new InsertBoltImpl(connectionProvider, listJdbcMapper)
                .withInsertQuery("insert into inv_list_d (" + ListObj.columnsToString() + ") values (" + ListObj.getPlaceholders() + ")")
                .withQueryTimeoutSecs(30);

        builder.setSpout(Topology.LIST_SPOUT.getId(), listAMQPSpout);
        builder.setBolt(Topology.LIST_TRANSFORM_BOLT.getId(), listTransformBolt)
                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.LIST.id());
        builder.setBolt(Topology.LIST_PERSIST_BOLT.getId(), listPersistenceBolt)
                .shuffleGrouping(Topology.LIST_TRANSFORM_BOLT.getId(), Streams.LIST.id());

        return builder;
    }


    /* Error Handling */
    /* Handle inserting errors to PDI_LOGGING, and failing the tuple gracefully */
    /* TODO: database to log to     */
    private static TopologyBuilder buildErrorTopology(TopologyBuilder builder) {
        ErrorBolt errorBolt = new ErrorBolt();
        builder.setBolt(Topology.ERROR_BOLT.getId(), errorBolt)
                .shuffleGrouping(Topology.PARSE_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.ITEM_TRANSFORM_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.ITEM_STATE_TRANSFORM_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.DROP_TRANSFORM_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.DROP_STATE_TRANSFORM_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.ITEM_PERSIST_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.DROP_PERSIST_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.DROP_STATE_PERSIST_BOLT.getId(), Streams.ERROR.id())
                .shuffleGrouping(Topology.ITEM_STATE_PERSIST_BOLT.getId(), Streams.ERROR.id());

        return builder;
    }
}


