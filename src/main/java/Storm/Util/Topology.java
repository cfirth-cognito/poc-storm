package Storm.Util;

/**
 * Created by charlie on 21/03/17.
 */
public enum Topology {
    TOPOLOGY_NAME("MI-STORM-V0.1"),
    ITEM_SPOUT("ItemAMQPSpout"),
    ITEM_STATE_SPOUT("ItemStateAMQPSpout"),
    DROP_SPOUT("DropAMQPSpout"),
    DROP_STATE_SPOUT("DropStateAMQPSpout"),
    LIST_SPOUT("ListAMQPSpout"),
    PARSE_BOLT("parse_amqp_bolt"),
    ITEM_TRANSFORM_BOLT("item_transform_bolt"),
    ITEM_STATE_TRANSFORM_BOLT("item_state_transform_bolt"),
    DROP_TRANSFORM_BOLT("drop_transform_bolt"),
    DROP_STATE_TRANSFORM_BOLT("drop_state_transform_bolt"),
    SEQUENCING_BOLT("sequencing_bolt"),
    ITEM_PERSIST_BOLT("item_persist_bolt");

    private String id;

    Topology(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
