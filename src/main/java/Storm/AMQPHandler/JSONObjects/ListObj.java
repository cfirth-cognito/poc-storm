package Storm.AMQPHandler.JSONObjects;

import Storm.Util.Field;

/**
 * Created by charlie on 17/02/17.
 */
public class ListObj {


    private String reference;
    private Field listClass = new Field();
    private Field listSubClass = new Field();
    private String listClassDisplay;
    private String listSubclassClassDisplay;
    private String eventDate;
    private Field beginDate = new Field();
    private Field schedule = new Field();
    private Field resource = new Field();
    private Field routeType = new Field();
    private String availableDate;
    private Integer sortOrder;


    public ListObj() {
    }

    public Field getListSubClass() {
        return listSubClass;
    }

    public void setListSubClass(Field listSubClass) {
        this.listSubClass = listSubClass;
    }

    public String getListSubclassClassDisplay() {
        return listSubclassClassDisplay;
    }

    public void setListSubclassClassDisplay(String listSubclassClassDisplay) {
        this.listSubclassClassDisplay = listSubclassClassDisplay;
    }

    public Field getResource() {
        return resource;
    }

    public void setResource(Field resource) {
        this.resource = resource;
    }

    public String getListClassDisplay() {
        return listClassDisplay;
    }

    public void setListClassDisplay(String listClassDisplay) {
        this.listClassDisplay = listClassDisplay;
    }

    public Integer getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(Integer sortOrder) {
        this.sortOrder = sortOrder;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
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

    public Field getSchedule() {
        return schedule;
    }

    public void setSchedule(Field schedule) {
        this.schedule = schedule;
    }

    public Field getRouteType() {
        return routeType;
    }

    public void setRouteType(Field routeType) {
        this.routeType = routeType;
    }

    public String getAvailableDate() {
        return availableDate;
    }

    public void setAvailableDate(String availableDate) {
        this.availableDate = availableDate;
    }
}
