package lambda.service2;

/**
 *
 * @author Wes Lloyd
 */
public class LoadRequest {
    private String bucketname;
    private String filename;
    private String name;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LoadRequest() {

    }

    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
