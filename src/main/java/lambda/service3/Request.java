package lambda.service3;


public class Request {

    private String region;

    private String itemType;

    private String salesChannel;

    private String orderPriority;

    private String country;

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getSalesChannel() {
        return salesChannel;
    }

    public void setSalesChannel(String salesChannel) {
        this.salesChannel = salesChannel;
    }

    public String getOrderPriority() {
        return orderPriority;
    }

    public void setOrderPriority(String orderPriority) {
        this.orderPriority = orderPriority;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return "Request{" +
                "Region='" + region + '\'' +
                ", ItemType='" + itemType + '\'' +
                ", SalesChannel='" + salesChannel + '\'' +
                ", OrderPriority='" + orderPriority + '\'' +
                ", Country='" + country + '\'' +
                '}';
    }
}
