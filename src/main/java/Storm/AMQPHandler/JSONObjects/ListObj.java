package Storm.AMQPHandler.JSONObjects;

import Storm.Util.Field;

/**
 * Created by charlie on 17/02/17.
 */
public class ListObj {


    private String reference;
    private Field type = new Field();
    private Field subType = new Field();
    private Field listClass = new Field();
    private String eventDate;
    private Field beginDate = new Field();
    private Field schedueleRef = new Field();
    private Field routeType = new Field();
    private Field resource = new Field();
    private Field vanRouteId = new Field();
    private String availableDate;


    public ListObj() {
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public Field getType() {
        return type;
    }

    public void setType(Field type) {
        this.type = type;
    }

    public Field getSubType() {
        return subType;
    }

    public void setSubType(Field subType) {
        this.subType = subType;
    }

    public Field getListClass() {
        return listClass;
    }

    public void setListClass(Field listClass) {
        this.listClass = listClass;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public Field getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Field beginDate) {
        this.beginDate = beginDate;
    }

    public Field getSchedueleRef() {
        return schedueleRef;
    }

    public void setSchedueleRef(Field schedueleRef) {
        this.schedueleRef = schedueleRef;
    }

    public Field getRouteType() {
        return routeType;
    }

    public void setRouteType(Field routeType) {
        this.routeType = routeType;
    }

    public Field getResource() {
        return resource;
    }

    public void setResource(Field resource) {
        this.resource = resource;
    }

    public Field getVanRouteId() {
        return vanRouteId;
    }

    public void setVanRouteId(Field vanRouteId) {
        this.vanRouteId = vanRouteId;
    }

    public String getAvailableDate() {
        return availableDate;
    }

    public void setAvailableDate(String availableDate) {
        this.availableDate = availableDate;
    }
}
