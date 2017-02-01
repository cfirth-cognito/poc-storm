package Storm.DatabaseHandler.InsertBolts;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by charlie on 31/01/17.
 */

/**
 * Created for use in our own JDBC implementation of the InsertBolt.
 * No use for this yet..but will become useful.
 */
public class InsertBoltImpl extends AbstractJdbcBolt {
    private String tableName;
    private String insertQuery;
    private JdbcMapper jdbcMapper;

    public InsertBoltImpl(ConnectionProvider connectionProvider, JdbcMapper jdbcMapper) {
        super(connectionProvider);
        Validate.notNull(jdbcMapper);
        this.jdbcMapper = jdbcMapper;
    }

    public InsertBoltImpl withInsertQuery(String insertQuery) {
        if (this.tableName != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        } else {
            this.insertQuery = insertQuery;
            return this;
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
    }

    public InsertBoltImpl withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }


    @Override
    public void execute(Tuple tuple) {
        try {
            List<Column> columns = this.jdbcMapper.getColumns(tuple);
            List<List<Column>> columnLists = new ArrayList();
            columnLists.add(columns);
            if (!StringUtils.isBlank(this.tableName)) {
                this.jdbcClient.insert(this.tableName, columnLists);
            } else {
                this.jdbcClient.executeInsertQuery(this.insertQuery, columnLists);
            }

            /* Returns the ID of the last insert executed by the current connection */
//            String insertId = (String) this.jdbcClient.select("SELECT LAST_INSERT_ID();", new ArrayList<>()).get(0).get(0).getVal();

//            this.collector.emit(tuple.getSourceStreamId(), new Values(insertId));
            this.collector.ack(tuple);
        } catch (Exception exception) {
            System.out.println("[LOG] Emitting to ErrorStream");
            if(exception.getCause() != null) {
                this.collector.emit("ErrorStream", new Values(String.format("Exception while attempting to insert record: %s", exception.getCause().getMessage())));
            } else {
                this.collector.emit("ErrorStream", new Values(String.format("Exception while attempting to insert record: %s", exception.getMessage())));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("ErrorStream", new Fields("error_msg"));
    }
}
