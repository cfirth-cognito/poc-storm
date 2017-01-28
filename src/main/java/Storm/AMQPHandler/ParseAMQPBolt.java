package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import Storm.AMQPHandler.JSONObj.Item.ItemPayload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Charlie on 28/01/2017.
 */
public class ParseAMQPBolt implements IRichBolt {

    private TopologyContext context;
    private OutputCollector outputCollector;

    private Fields fields;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        /* Include Type in tuple. switch() this method based on type, to parse the correct msgtype, and emit it */
        String msgBody = tuple.getStringByField("body");

        Item item = new Item();
        item.setReference(parseByPath(msgBody, ".itemMetadata.reference"));
        item.setItemClass(parseByPath(msgBody, ".itemMetadata.class"));
        item.setItemSubClass(parseByPath(msgBody, ".itemMetadata.subclass"));
        item.setCustomerName(parseByPath(msgBody, ".itemMetadata"));
        item.setCustomerName(parseByPath(msgBody, "$.contactDetails.name"));
        item.setClient(parseByPath(msgBody, "$.itemMetadata.parameters.ClientId"));
        item.setStatedDay(parseByPath(msgBody, "$.itemMetadata.parameters.StatedDay"));
        item.setStatedTime(parseByPath(msgBody, "$.itemMetadata.parameters.StatedTime"));
        item.setEventDate(parseByPath(msgBody, "$.itemMetadata.eventDate"));
        item.setRouteRef(parseByPath(msgBody, "$.routeReference"));
        item.setRouteType(parseByPath(msgBody, "$.routeType"));
        item.setLineItemId(parseByPath(msgBody, "$.itemMetadata.lineItemViews&#x5b;0&#x5d;.id"));
        item.setCustAddr(parseByPath(msgBody, "$.customerLocation.address.line1"));
        item.setPostcode(parseByPath(msgBody, "$.customerLocation.address.postCode"));
        item.setShopReference(parseByPath(msgBody, "$.parcelShopLocation.reference"));
        List<Object> output = new ArrayList<>();
        output.add(item);
        System.out.println(item);
        outputCollector.emit("parse_amqp", output);
    }

    private String parseByPath(String msg, String path) {
        String payload = "$.payload";
        return JsonPath.parse(msg).read(payload + path);
    }


    @Override
    public void cleanup() {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("item"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
