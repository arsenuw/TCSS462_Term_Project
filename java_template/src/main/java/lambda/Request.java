package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;

    private String readBucketName;
    private String writeBucketName;

    private String readFilename;
    private String writeFilename;
    private int row;
    private int col;


    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }

    public String getReadBucketName(){
        return readBucketName;
    }

    public void setReadBucketName(String readBucketName){
        this.readBucketName = readBucketName;
    }

    public String getWriteBucketName(){
        return writeBucketName;
    }

    public void setWriteBucketName(String writeBucketName){
        this.writeBucketName = writeBucketName;
    }

    public String getReadilename(){
        return readFilename;
    }

    public void setReadFilename(String filename){
        this.readFilename = filename;
    }

    public String getWriteFilename(){
        return writeFilename;
    }

    public void setWriteFilename(String filename){
        this.writeFilename = filename;
    }

    public int getCol(){
        return col;
    }

    public void setCol(int col){
        this.col = col;
    }

    public int getRow(){
        return row;
    }

    public void setRow(int row){
        this.row = row;
    }
}
