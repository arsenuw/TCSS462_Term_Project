package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    private String name; 
    
    private  String bucketname; 
    private String filename; 
    private String transformName;

    public String getTransformName() {
        return transformName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
    }
    private int row; 
    private int col; 
    
    private String database;
    private String sqlbucketname; 
    private String sqlname;

    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return getName().toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }

    /**
     * @return the bucketname
     */
    public String getBucketname() {
        return bucketname;
    }

    /**
     * @param bucketname the bucketname to set
     */
    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    } 

    public  String getSQLbucketName() { 
        return sqlbucketname;
    }

    public void setSQLBucketName(String sqlbucketname) { 
        this.sqlbucketname = sqlbucketname;
    } 
    
    public String getsqlname() { 
    	return sqlname;
	} 
	
    public void setsqlname(String name) { 
    sqlname = name;
   }

    /**
     * @return the row
     */
    public int getRow() {
        return row;
    }

    /**
     * @param row the row to set
     */
    public void setRow(int row) {
        this.row = row;
    }

    /**
     * @return the col
     */
    public int getCol() {
        return col;
    }

    /**
     * @param col the col to set
     */
    public void setCol(int col) {
        this.col = col;
    }
}
