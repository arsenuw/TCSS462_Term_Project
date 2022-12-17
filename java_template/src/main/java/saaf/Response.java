package saaf;

import java.util.LinkedList;

/**
 * A basic Response object that can be consumed by FaaS Inspector
 * to be used as additional output.
 * 
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Response {
    private String fileName;
    public LinkedList<String> names;
    private String bucketName;
    
    
    public String getBucket() {
        return bucketName;
    }

    public String getName() {
        return fileName;
    }

    public void setName(String value) {
        this.fileName = value;
    }
    
    public void setBucket(String b) {
        this.bucketName = b;
    }
    
    public void setNames(LinkedList<String> names) {
        this.names = names;
    }

    public String getNamesString() {
        StringBuilder sb = new StringBuilder();
        for (String s : this.names) {
            sb.append(s + "; ");
        }
        return sb.toString();
    }
  
    @Override
    public String toString() {
        return "bucket=" + this.getBucket() + ", filename=" + this.getName() + ", CSVresults=" + this.getNamesString() + super.toString();
    }
}
