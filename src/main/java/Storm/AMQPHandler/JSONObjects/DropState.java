package Storm.AMQPHandler.JSONObjects;

import Storm.Util.Field;

/**
 * Created by Charlie on 28/01/2017.
 */

public class DropState {

    private String reference;
    private String eventDate;
    private Integer stateDateId;
    private Integer stateTimeId;
    private String messageRef;
    private Integer dropID;
    private Field list = new Field();
    private Field dropClass = new Field();
    private Field dropSubClass = new Field();
    private Field dropStateClass = new Field();
    private Field dropStateSubClass = new Field();
    private Field resource = new Field();
    private Field schedule = new Field();
    private int networkId;
    private int geographyId;
    private int stateCounter;
    private Field beginDate = new Field();
    private Field dropStatus = new Field();
    private Field manifested = new Field();
    private Field trackingPoint = new Field();
    private Field routeType = new Field();
    private String billingRef;
    private Field shop = new Field();
    private Field route = new Field();
    private String lat;
    private String longitude;
    private String validity;
    private String duration;
    private String customerName;
    private String designRank;
    private Field outcomeClass = new Field();
    private Field outcomeSubClass = new Field();

    public DropState() {
    }

    public String getDesignRank() {
        return designRank;
    }

    public void setDesignRank(String designRank) {
        this.designRank = designRank;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public Integer getStateDateId() {
        return stateDateId;
    }

    public void setStateDateId(Integer stateDateId) {
        this.stateDateId = stateDateId;
    }

    public Integer getStateTimeId() {
        return stateTimeId;
    }

    public void setStateTimeId(Integer stateTimeId) {
        this.stateTimeId = stateTimeId;
    }

    public String getMessageRef() {
        return messageRef;
    }

    public void setMessageRef(String messageRef) {
        this.messageRef = messageRef;
    }

    public Integer getDropID() {
        return dropID;
    }

    public void setDropID(Integer dropID) {
        this.dropID = dropID;
    }

    public Field getList() {
        return list;
    }

    public void setList(Field list) {
        this.list = list;
    }

    public Field getDropClass() {
        return dropClass;
    }

    public void setDropClass(Field dropClass) {
        this.dropClass = dropClass;
    }

    public Field getDropSubClass() {
        return dropSubClass;
    }

    public void setDropSubClass(Field dropSubClass) {
        this.dropSubClass = dropSubClass;
    }

    public Field getDropStateClass() {
        return dropStateClass;
    }

    public void setDropStateClass(Field dropStateClass) {
        this.dropStateClass = dropStateClass;
    }

    public Field getDropStateSubClass() {
        return dropStateSubClass;
    }

    public void setDropStateSubClass(Field dropStateSubClass) {
        this.dropStateSubClass = dropStateSubClass;
    }

    public Field getResource() {
        return resource;
    }

    public void setResource(Field resource) {
        this.resource = resource;
    }

    public Field getSchedule() {
        return schedule;
    }

    public void setSchedule(Field schedule) {
        this.schedule = schedule;
    }

    public int getNetworkId() {
        return networkId;
    }

    public void setNetworkId(int networkId) {
        this.networkId = networkId;
    }

    public int getGeographyId() {
        return geographyId;
    }

    public void setGeographyId(int geographyId) {
        this.geographyId = geographyId;
    }

    public int getStateCounter() {
        return stateCounter;
    }

    public void setStateCounter(int stateCounter) {
        this.stateCounter = stateCounter;
    }

    public Field getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Field beginDate) {
        this.beginDate = beginDate;
    }

    public Field getDropStatus() {
        return dropStatus;
    }

    public void setDropStatus(Field dropStatus) {
        this.dropStatus = dropStatus;
    }

    public Field getManifested() {
        return manifested;
    }

    public void setManifested(Field manifested) {
        this.manifested = manifested;
    }

    public Field getTrackingPoint() {
        return trackingPoint;
    }

    public void setTrackingPoint(Field trackingPoint) {
        this.trackingPoint = trackingPoint;
    }

    public Field getRouteType() {
        return routeType;
    }

    public void setRouteType(Field routeType) {
        this.routeType = routeType;
    }

    public String getBillingRef() {
        return billingRef;
    }

    public void setBillingRef(String billingRef) {
        this.billingRef = billingRef;
    }

    public Field getShop() {
        return shop;
    }

    public void setShop(Field shop) {
        this.shop = shop;
    }

    public Field getRoute() {
        return route;
    }

    public void setRoute(Field route) {
        this.route = route;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getValidity() {
        return validity;
    }

    public void setValidity(String validity) {
        this.validity = validity;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public Field getOutcomeClass() {
        return outcomeClass;
    }

    public void setOutcomeClass(Field outcomeClass) {
        this.outcomeClass = outcomeClass;
    }

    public Field getOutcomeSubClass() {
        return outcomeSubClass;
    }

    public void setOutcomeSubClass(Field outcomeSubClass) {
        this.outcomeSubClass = outcomeSubClass;
    }
}
