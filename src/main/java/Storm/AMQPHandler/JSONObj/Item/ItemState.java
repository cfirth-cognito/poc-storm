package Storm.AMQPHandler.JSONObj.Item;

import Storm.Util.Field;

/**
 * Created by Charlie on 28/01/2017.
 */
public class ItemState {

    private String reference;
    private String stateDateTimeLocal;
    private Integer stateDateId;
    private Integer stateTimeId;
    private String messageRef;
    private Integer itemId;
    private Integer listId;
    private String listRef;
    private Field itemClass = new Field();
    private Field itemSubClass = new Field();
    private Field itemStateClass = new Field();
    private Field itemStateSubClass = new Field();
    private int resourceId;
    private int scheduleId;
    private String customerName;
    private int networkId;
    private int geographyId;
    private int stateCounter;
    private String beginDate;
    private String etaStartDate;
    private String etaEndDate;
    private String additionalInfo;
    private String status;
    private int statusId;
    private Field manifested = new Field();
    private String routeType;
    private int routeTypeId;
    private String fromShop;
    private int fromShopId;
    private String toShop;
    private int toShopId;
    private int beginDateId;
    private String billingRef;

    private String resourceRef;
    private String routeRef;
    private Field outcomeClass = new Field();
    private Field outcomeSubClass = new Field();
    private Field trackingPoint = new Field();
    private Field shopReference = new Field();

    private int clientId;

    public ItemState() {
    }


    public Field getItemClass() {
        return itemClass;
    }

    public void setItemClass(Field itemClass) {
        this.itemClass = itemClass;
    }

    public Field getItemSubClass() {
        return itemSubClass;
    }

    public void setItemSubClass(Field itemSubClass) {
        this.itemSubClass = itemSubClass;
    }

    public Field getItemStateClass() {
        return itemStateClass;
    }

    public void setItemStateClass(Field itemStateClass) {
        this.itemStateClass = itemStateClass;
    }

    public Field getItemStateSubClass() {
        return itemStateSubClass;
    }

    public void setItemStateSubClass(Field itemStateSubClass) {
        this.itemStateSubClass = itemStateSubClass;
    }

    public Field getShopReference() {
        return shopReference;
    }

    public void setShopReference(Field shopReference) {
        this.shopReference = shopReference;
    }

    public Field getTrackingPoint() {
        return trackingPoint;
    }

    public void setTrackingPoint(Field trackingPoint) {
        this.trackingPoint = trackingPoint;
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

    public String getRouteRef() {
        return routeRef;
    }

    public void setRouteRef(String routeRef) {
        this.routeRef = routeRef;
    }

    public String getResourceRef() {
        return resourceRef;
    }

    public void setResourceRef(String resourceRef) {
        this.resourceRef = resourceRef;
    }

    public int getBeginDateId() {
        return beginDateId;
    }

    public void setBeginDateId(int beginDateId) {
        this.beginDateId = beginDateId;
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    public String getStateDateTimeLocal() {
        return stateDateTimeLocal;
    }

    public void setStateDateTimeLocal(String stateDateTimeLocal) {
        this.stateDateTimeLocal = stateDateTimeLocal;
    }


    public String getRouteType() {
        return routeType;
    }

    public void setRouteType(String routeType) {
        this.routeType = routeType;
    }


    public int getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(int scheduleId) {
        this.scheduleId = scheduleId;
    }

    public int getResourceId() {
        return resourceId;
    }

    public void setResourceId(int resourceId) {
        this.resourceId = resourceId;
    }

    public String getMessageRef() {
        return messageRef;
    }

    public void setMessageRef(String messageRef) {
        this.messageRef = messageRef;
    }

    public Integer getItemId() {
        return itemId;
    }

    public void setItemId(Integer itemId) {
        this.itemId = itemId;
    }

    public Integer getListId() {
        return listId;
    }

    public void setListId(Integer listId) {
        this.listId = listId;
    }

    public String getListRef() {
        return listRef;
    }

    public void setListRef(String listRef) {
        this.listRef = listRef;
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

    public String getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public String getEtaStartDate() {
        return etaStartDate;
    }

    public void setEtaStartDate(String etaStartDate) {
        this.etaStartDate = etaStartDate;
    }

    public String getEtaEndDate() {
        return etaEndDate;
    }

    public void setEtaEndDate(String etaEndDate) {
        this.etaEndDate = etaEndDate;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public int getStatusId() {
        return statusId;
    }

    public void setStatusId(int statusId) {
        this.statusId = statusId;
    }

    public int getRouteTypeId() {
        return routeTypeId;
    }

    public void setRouteTypeId(int routeTypeId) {
        this.routeTypeId = routeTypeId;
    }

    public String getFromShop() {
        return fromShop;
    }

    public void setFromShop(String fromShop) {
        this.fromShop = fromShop;
    }

    public int getFromShopId() {
        return fromShopId;
    }

    public void setFromShopId(int fromShopId) {
        this.fromShopId = fromShopId;
    }

    public String getToShop() {
        return toShop;
    }

    public void setToShop(String toShop) {
        this.toShop = toShop;
    }

    public int getToShopId() {
        return toShopId;
    }

    public void setToShopId(int toShopId) {
        this.toShopId = toShopId;
    }

    public String getBillingRef() {
        return billingRef;
    }

    public void setBillingRef(String billingRef) {
        this.billingRef = billingRef;
    }

    public Field getManifested() {
        return manifested;
    }

    public void setManifested(Field manifested) {
        this.manifested = manifested;
    }
}
