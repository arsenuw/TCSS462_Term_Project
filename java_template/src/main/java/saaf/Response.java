package saaf;

/**
 * A basic Response object that can be consumed by FaaS Inspector
 * to be used as additional output.
 * 
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Response {
    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //

    // Return value
    private String value;
   // private String bucketName; 
   // private String fileName;  
   /* 
    public void setBucket(String b) {
        this.bucketName = b;
    }

    public String getBucket() {
        return bucketName;
    } */

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    } 
    /* 
    public String getName() {
        return fileName;
    }

    public void setName(String value) {
        this.fileName = value;
    } */

    @Override
    public String toString() { 
       // return "bucket=" + this.getBucket() + ", filename=" + this.getName() + super.toString();
        return "value=" + this.getValue() + super.toString();
    }
}
