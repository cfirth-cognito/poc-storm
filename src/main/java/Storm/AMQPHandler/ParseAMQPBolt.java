package Storm.AMQPHandler;

import Storm.AMQPHandler.JSONObj.Item.Item;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

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

        System.out.println("CF Parsing AMQP message..");

        Item item = new Item();

//        System.out.println("CF " + JsonPath.parse(msgBody).read("$.payload"));

        String payload = JsonPath.parse(msgBody).read("$.payload");

        item.setReference(parseByPath(payload, ".itemMetadata.reference"));
        item.setItemClass(parseByPath(payload, ".itemMetadata.class"));
        item.setItemSubClass(parseByPath(payload, ".itemMetadata.subClass"));
        item.setCustomerName(parseByPath(payload, ".contactDetails.name"));
        item.setClient(parseByPath(payload, ".itemMetadata.parameters.ClientId"));
        item.setStatedDay(parseByPath(payload, ".itemMetadata.parameters.StatedDay"));
        item.setStatedTime(parseByPath(payload, ".itemMetadata.parameters.StatedTime"));
        item.setEventDate(parseByPath(payload, ".itemMetadata.eventDate"));
        item.setRouteRef(parseByPath(payload, ".routeReference"));
        item.setRouteType(parseByPath(payload, ".routeType"));
        item.setLineItemId(parseByPath(payload, ".itemMetadata.lineItemViews[0].id"));
        item.setCustAddr(parseByPath(payload, ".customerLocation.address.line1"));
        item.setPostcode(parseByPath(payload, ".customerLocation.address.postCode"));
        item.setShopReference(parseByPath(payload, ".parcelShopLocation.reference"));
        List<Object> output = new ArrayList<>();
        output.add(item);
        System.out.println(item);
        System.out.println(item.toString());

        System.out.println("CF Item created.");

        // Field Specific grouping
        // To lookup classes
        outputCollector.emit(tuple, output);

        outputCollector.ack(tuple);
    }

    private String parseByPath(String msg, String path) {
        String payload = "$";
        try {
            return JsonPath.parse(msg).read(payload + path);
        } catch (PathNotFoundException e) {
            System.out.println("CF CAUGHT EXCEPTION");
            System.out.println("CF " + e.getMessage());
            if (e.getMessage().contains("'null")) { // Null value in JSON - valid, handle properly
                return null;
            } else {
                e.printStackTrace();
            }
        }
        return null;
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
