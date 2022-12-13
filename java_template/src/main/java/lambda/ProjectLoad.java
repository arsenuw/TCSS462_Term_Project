package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.codec.net.QCodec;

import java.util.Scanner;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object; 
import java.io.StringWriter; 
import java.io.ByteArrayInputStream;  
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import com.amazonaws.services.s3.model.ObjectMetadata;
//import java.io.ByteArrayInputStream;
import java.io.InputStream; 
import java.util.Scanner;
/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class ProjectLoad implements RequestHandler<Request, HashMap<String, Object>> {


    static int uses = 0;
    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll(); 

        String bucketname = request.getBucketname(); 
        String filename = request.getFilename();    
       // String sqlbucketname= request.getSQLbucketName(); 
        //String sqlname = request.getsqlname();  
        String sqlbucketname = "term-project-bucket-462";
        String sqlname = "mytest.db";

        String srcBucket = bucketname; 
        String srcKey = filename;  
 
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build(); 
    //get object file using source bucket and srcKey name
    S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));

        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();
        
        String pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);

        logger.log("set pwd to tmp");        
        setCurrentDirectory("/tmp");
        
        pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);
        r.setVersion("version");
        try
        {
            // Connection string an in-memory SQLite DB
           // Connection con = DriverManager.getConnection("jdbc:sqlite:"); 

            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:/tmp/mytest.db");

            // Detect if the table 'mytable' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='mytable'");
            ResultSet rs = ps.executeQuery();
            if (!rs.next())
            {
                // 'mytable' does not exist, and should be created
                logger.log("trying to create table 'mytable'");
              //  ps = con.prepareStatement("CREATE TABLE mytable ( name text, col2 text, col3 text);"); 
                ps = con.prepareStatement("CREATE TABLE mytable ( Region text, Country text, ItemType text, SalesChannel text,OrderPrriority"  
                +"text,OrderDate date,orderID text,ShipDate date,UnitsSold text, UnitPrice text,UnitCost text, TotalRevenue text,TotalCost text, TotalProfit Text);"); 


                ps.execute();
            }
            rs.close();
                /* 
            // Insert row into mytable
            ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','" +
                 UUID.randomUUID().toString().substring(0,8) + "','" + UUID.randomUUID().toString().substring(0,4) + "');");
            ps.execute(); */
            InputStream objectData = s3Object.getObjectContent();
            Scanner scanner = new Scanner(objectData);  
            /* 
            int index =0; 
            ArrayList<String> list = new ArrayList<String>();
            while(scanner.hasNextLine()) {  
                if(index ==0) { 
                    index++;
                } else {   
                    list.set(index, scanner.nextLine()); 
                    index++;
                  //  List.get(first)
                    
                }

            } 
            for(int i=0; i<list.size(); i++) { 
                String[] temp= list.get(i).split(",");  

                String statement = "insert into my table values ('";
                for(int j=0; j<temp.length; j++) { 
                    statement+=temp[i];  
                    if(j==temp.length-1) {
                   // statement+="','";      
                    }        else  {  
                        statement+="','";
                    }    
                } 
                statement+= "');";  
                ps = con.prepareStatement(statement);
                ps.execute();
            }   */

            // better implementation 
            String sqlinfo = "insert into mytable "; 
            sqlinfo+= "(Region text, Country text, ItemType text, SalesChannel text,OrderPriority"  
            +"text,OrderDate date,orderID text,ShipDate date,UnitsSold text, UnitPrice text,UnitCost text, TotalRevenue text,TotalCost text, TotalProfit Text)"; 
            sqlinfo+="VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";  
            int temp =0;
            while(scanner.hasNextLine()) {  
                if(temp !=0) { 
                String[] arr = scanner.nextLine().split(","); 
                ps = con.prepareStatement(sqlinfo); 
                ps.setString(1,arr[0]);  
                ps.setString(2,arr[1]);  
                ps.setString(3,arr[2]);  
                ps.setString(4,arr[3]); 
                ps.setString(5,arr[4]); 
                ps.setString(6,arr[5]);  
                ps.setString(7,arr[6]);  
                ps.setString(8,arr[7]);  
                ps.setString(9,arr[8]);  
                ps.setString(10,arr[9]); 
                ps.setString(11,arr[10]); 
                ps.setString(12,arr[11]); 
                ps.setString(13,arr[12]); 
                ps.setString(14,arr[13]);  
                ps.execute(); 
                } 
                temp++;
            }
                scanner.close();
             

            // Query mytable to obtain full resultset
            ps = con.prepareStatement("select * from mytable;");
            rs = ps.executeQuery();

            // Load query results for [name] column into a Java Linked List
            // ignore [col2] and [col3] 
            LinkedList<String> ll = new LinkedList<String>();
            while (rs.next())
            {
                logger.log("name=" + rs.getString("Region"));
                ll.add(rs.getString("Region"));
               // logger.log("col2=" + rs.getString("col2"));
              //  logger.log("col3=" + rs.getString("col3"));
            }
            rs.close();
            con.close();  

            r.setNames(ll); 
            
            // sleep to ensure that concurrent calls obtain separate Lambdas
            try
            {
                Thread.sleep(200);
            }
            catch (InterruptedException ie)
            {
                logger.log("interrupted while sleeping...");
            }
        }
        catch (SQLException sqle)
        {
            logger.log("DB ERROR:" + sqle.toString());
            sqle.printStackTrace();
        }

        // *********************************************************************
        // Set hello message here
        // ********************************************************************* 
        uses=uses+1;
        String hello = "Hello " + request.getName()+" calls="+uses;

        // Set return result in Response class, class is marshalled into JSON
        r.setValue(hello);
        
        inspector.consumeResponse(r);    
       ObjectMetadata meta = new ObjectMetadata();
        try {   
            File stuff = new File(sqlname);
            InputStream is = new FileInputStream(stuff);    
            meta.setContentLength(stuff.length());
            meta.setContentType("text/plain");
            s3Client.putObject(sqlbucketname, sqlname, is, meta); 
        } 
        catch(FileNotFoundException e) { 
            System.out.println("impossible");
        }
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
    public static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File    directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }


    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        // Create an instance of the class 
        ProjectLoad lt = new ProjectLoad();
        //HelloSqlite lt = new HelloSqlite();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        req.setName(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getName());

        // Run the function
        HashMap resp = lt.handleRequest(req, c);
        try
        {
            Thread.sleep(10);
        }
        catch (InterruptedException ie)
        {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }

    
}
