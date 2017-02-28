package Storm.Util;

/**
 * Created by charlie on 28/02/17.
 */
public enum Streams {
    ERROR("ErrorStream"),
    ITEM("item"),
    ITEM_STATE("item-state"),
    DROP("drop"),
    DROP_STATE("drop-state");

    private String streamId;

    Streams(String streamId) {
        this.streamId = streamId;
    }

    public String id() {
        return streamId;
    }
}
