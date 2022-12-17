package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.UUID;


/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class service3 implements RequestHandler<Request, HashMap<String, Object>> {

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
        AmazonS3 s3ClientRead = AmazonS3ClientBuilder.standard().build();   
        
       // s3ClientRead.setRegion(Regions.US_WEST_2);
       // S3Object s3Object = s3ClientRead.getObject(new GetObjectRequest(bucketname, filename));
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();
        r.setVersion("version");
        String pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);

        logger.log("set pwd to tmp");        
        setCurrentDirectory("/tmp");
        File local = new File("/tmp/mytest.db");
        ObjectMetadata s3Object = s3ClientRead.getObject(new GetObjectRequest(bucketname, filename),local);  
      //S3Object s3Object = s3ClientRead.getObject(new GetObjectRequest(bucketname, filename));
        pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);
        
        try
        {
            // Connection string an in-memory SQLite DB
           // Connection con = DriverManager.getConnection("jdbc:sqlite:"); 

            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:/tmp/mytest.db");

            // Detect if the table 'mytable' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='mytable'");
            ResultSet rs = ps.executeQuery();  
    
            /* 
            if (!rs.next())
            {
                // 'mytable' does not exist, and should be created
                logger.log("trying to create table 'mytable'");
                ps = con.prepareStatement("CREATE TABLE mytable ( name text, col2 text, col3 text);");
                ps.execute();
            } */
          //  rs.close();

            // Insert row into mytable
           // ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','" +
            //     UUID.randomUUID().toString().substring(0,8) + "','" + UUID.randomUUID().toString().substring(0,4) + "');");
            //ps.execute();

            // Query mytable to obtain full resultset
            ps = con.prepareStatement("select * from mytable;");
            rs = ps.executeQuery();

            // Load query results for [name] column into a Java Linked List
            // ignore [col2] and [col3]  
             
            LinkedList<String> ll = new LinkedList<String>();
            while (rs.next())
            {
                logger.log("name=" + rs.getString("region"));
              //  ll.add(rs.getString("name"));
              //  logger.log("col2=" + rs.getString("col2"));
               // logger.log("col3=" + rs.getString("col3"));
            } 
           // rs.close();
            /* 
            //Average [Gross Margin] in percent
            ps = con.prepareStatement("select AVG(Gross Margin) * 100 / sum(Gross Margin) from table;");
            rs = ps.executeQuery();

            //Average [Units Sold]
            ps = con.prepareStatement("select AVG(Units Sold)  from table;");
            rs = ps.executeQuery();

            //Max [Units Sold]
            ps = con.prepareStatement("select MAX(Units Sold)  from table;");
            rs = ps.executeQuery();

            //Min [Units Sold]
            ps = con.prepareStatement("select MIN(Units Sold)  from table;");
            rs = ps.executeQuery();

            //Total [Units Sold]
            ps = con.prepareStatement("select SUM(Units Sold)  from table;");
            rs = ps.executeQuery();

            //Total [Total Revenue]
            ps = con.prepareStatement("select SUM(Total Revenue)  from table;");
            rs = ps.executeQuery();

            //Total [Total Profit]
            ps = con.prepareStatement("select SUM(Total Profit)  from table;");
            rs = ps.executeQuery();

            //Number of Orders
            ps = con.prepareStatement("select COUNT(OrderID)  from table;");
            rs = ps.executeQuery(); */

            //con.close();  

            r.setNames(ll);
            rs.close(); 
            con.close();
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
        uses = uses + 1;
        String hello = "Hello " + request.getName() + " calls=" + uses;

        // Set return result in Response class, class is marshalled into JSON
        r.setValue(hello);
        
        inspector.consumeResponse(r);
        
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
        service3 lt = new service3();

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