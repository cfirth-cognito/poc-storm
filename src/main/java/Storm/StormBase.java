package Storm;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */

public class StormBase {
    public static void main(String[] args) {
        //AMQP Spout


        // Construct JDBC Persistence
        Map<String, Object> configMap = new HashMap<>();

        configMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("dataSource.url", "jdbc:mysql://localhost/hermes_mi");
        configMap.put("dataSource.user", "root");
        configMap.put("dataSource.password", "root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(configMap);

        String tableName = "user_details";
        List<Column> columns = Lists.newArrayList(new Column("column1", Types.INTEGER));
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columns);

        // Requires custom implementaion of JdbcMapper listing columns to be used
//        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
//                .withTableName("user")
//                .withQueryTimeoutSecs(30);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into inv_item_d (col, col) values (?,?)")
                .withQueryTimeoutSecs(30);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(null);
        builder.setBolt("lookupbolt");
        builder.setBolt("persistencebolt");
        builder.createTopology();


    }


}


}
