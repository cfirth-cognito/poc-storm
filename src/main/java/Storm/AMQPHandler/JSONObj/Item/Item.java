package Storm.AMQPHandler.JSONObj.Item;

/**
 * Created by Charlie on 28/01/2017.
 */
public class Item {

    private String reference;
    private String itemClass;
    private String itemSubClass;
    private String itemClassDisplay;
    private String itemSubClassDisplay;
    private String customerName;
    private String status;
    private String client;
    private String statedDay;
    private String statedTime;
    private String eventDate;
    private String routeRef;
    private String routeType;
    private String lineItemId;
    private String custAddr;
    private String postcode;
    private String shopReference;
    private int scheduleId;
    private int resourceId;
    private int clientId;

    public Item() {
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public String getItemClassDisplay() {
        return itemClassDisplay;
    }

    public void setItemClassDisplay(String itemClassDisplay) {
        this.itemClassDisplay = itemClassDisplay;
    }

    public String getItemSubClassDisplay() {
        return itemSubClassDisplay;
    }

    public void setItemSubClassDisplay(String itemSubClassDisplay) {
        this.itemSubClassDisplay = itemSubClassDisplay;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getItemClass() {
        return itemClass;
    }

    public void setItemClass(String itemClass) {
        this.itemClass = itemClass;
    }

    public String getItemSubClass() {
        return itemSubClass;
    }

    public void setItemSubClass(String itemSubClass) {
        this.itemSubClass = itemSubClass;
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

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getStatedDay() {
        return statedDay;
    }

    public void setStatedDay(String statedDay) {
        this.statedDay = statedDay;
    }

    public String getStatedTime() {
        return statedTime;
    }

    public void setStatedTime(String statedTime) {
        this.statedTime = statedTime;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public String getRouteRef() {
        return routeRef;
    }

    public void setRouteRef(String routeRef) {
        this.routeRef = routeRef;
    }

    public String getRouteType() {
        return routeType;
    }

    public void setRouteType(String routeType) {
        this.routeType = routeType;
    }

    public String getLineItemId() {
        return lineItemId;
    }

    public void setLineItemId(String lineItemId) {
        this.lineItemId = lineItemId;
    }

    public String getCustAddr() {
        return custAddr;
    }

    public void setCustAddr(String custAddr) {
        this.custAddr = custAddr;
    }

    public String getPostcode() {
        return postcode;
    }

    public void setPostcode(String postcode) {
        this.postcode = postcode;
    }

    public String getShopReference() {
        return shopReference;
    }

    public void setShopReference(String shopReference) {
        this.shopReference = shopReference;
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
}
