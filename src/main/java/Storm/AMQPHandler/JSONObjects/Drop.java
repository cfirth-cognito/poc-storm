package Storm.AMQPHandler.JSONObjects;

import Storm.Util.Field;

/**
 * Created by Charlie on 28/01/2017.
 */
public class Drop {

    private String reference;
    private Field dropClass = new Field();
    private Field dropSubClass = new Field();
    private String dropClassDisplay;
    private String dropSubClassDisplay;
    private Field status = new Field();
    private Field statusDisplay = new Field();
    private String eventDate;
    private Field route = new Field();
    private String routeType;
    private Field shopId = new Field();

    public Drop() {
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
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

    public String getDropClassDisplay() {
        return dropClassDisplay;
    }

    public void setDropClassDisplay(String dropClassDisplay) {
        this.dropClassDisplay = dropClassDisplay;
    }

    public String getDropSubClassDisplay() {
        return dropSubClassDisplay;
    }

    public void setDropSubClassDisplay(String dropSubClassDisplay) {
        this.dropSubClassDisplay = dropSubClassDisplay;
    }

    public Field getStatus() {
        return status;
    }

    public void setStatus(Field status) {
        this.status = status;
    }

    public Field getStatusDisplay() {
        return statusDisplay;
    }

    public void setStatusDisplay(Field statusDisplay) {
        this.statusDisplay = statusDisplay;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public Field getRoute() {
        return route;
    }

    public void setRoute(Field route) {
        this.route = route;
    }

    public String getRouteType() {
        return routeType;
    }

    public void setRouteType(String routeType) {
        this.routeType = routeType;
    }

    public Field getShopId() {
        return shopId;
    }

    public void setShopId(Field shopId) {
        this.shopId = shopId;
    }
}
