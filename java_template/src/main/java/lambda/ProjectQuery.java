package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.HashMap;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class ProjectQuery implements RequestHandler<Request, HashMap<String, Object>> {

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

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();
        r.setVersion("version");

        setCurrentDirectory("/tmp");
        File local = new File("/tmp/mytest.db");
        ObjectMetadata s3Object = s3ClientRead.getObject(new GetObjectRequest(bucketname, filename), local);
        //S3Object s3Object = s3ClientRead.getObject(new GetObjectRequest(bucketname, filename));        logger.log("pwd=" + pwd);

        try {
            LinkedList<String> ll = new LinkedList<String>();
            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:/tmp/mytest.db");

            //Average [Gross Margin] in percent
            PreparedStatement ps = con.prepareStatement("select (avg(grossmargin)*100/sum(grossmargin)) as grossMarginAvg from mytable group by region;");
            ResultSet rs = ps.executeQuery();
            ll.add("orderProcessingTime");
            while (rs.next()) {
                ll.add(rs.getString("grossMarginAvg"));
            }
            rs.close();

            //Average [Units Sold]
            ps = con.prepareStatement("select AVG(UnitsSold) as unitsSoldAvg from mytable;");
            rs = ps.executeQuery();
            ll.add("unitsSoldAvg");
            while (rs.next()) {
                ll.add(rs.getString("unitsSoldAvg"));
            }
            rs.close();

            //Max [Units Sold]
            ps = con.prepareStatement("select MAX(UnitsSold) as unitsSoldMax from mytable;");
            rs = ps.executeQuery();
            ll.add("unitsSoldMax");
            while (rs.next()) {
                ll.add(rs.getString("unitsSoldMax"));
            }
            rs.close();

            //Min [Units Sold]
            ps = con.prepareStatement("select MIN(UnitsSold) as unitsSoldMin from mytable;");
            rs = ps.executeQuery();
            ll.add("unitsSoldMin");
            while (rs.next()) {
                ll.add(rs.getString("unitsSoldMin"));
            }
            rs.close();

            //Total [Units Sold]
            ps = con.prepareStatement("select SUM(UnitsSold) as unitsSoldTotal from mytable;");
            rs = ps.executeQuery();
            ll.add("unitsSoldTotal");
            while (rs.next()) {
                ll.add(rs.getString("unitsSoldTotal"));
            }
            rs.close();

            //Total [Total Revenue]
            ps = con.prepareStatement("SELECT sum(totalrevenue) as revenueTotal from mytable WHERE orderpriority = 'Low' GROUP BY orderpriority;");
            rs = ps.executeQuery();
            ll.add("revenueTotal");
            while (rs.next()) {
                ll.add(rs.getString("revenueTotal"));
            }
            rs.close();

            //Total [Total Profit]
            ps = con.prepareStatement("SELECT sum(totalprofit) as profitTotal from mytable WHERE totalprofit > 1000 GROUP BY totalprofit;");
            rs = ps.executeQuery();
            ll.add("profitTotal");
            while (rs.next()) {
                ll.add(rs.getString("profitTotal"));
            }
            rs.close();

            //Number of Orders
            ps = con.prepareStatement("SELECT count(orderid) as ordersTotal FROM mytable WHERE orderid != 'Order ID';");
            rs = ps.executeQuery();
            ll.add("ordersTotal");
            while (rs.next()) {
                ll.add(rs.getString("ordersTotal"));
            }
            rs.close();

            //con.close();  
            r.setNames(ll);
            con.close();
            // sleep to ensure that concurrent calls obtain separate Lambdas
            try {
                Thread.sleep(200);
            } catch (InterruptedException ie) {
                logger.log("interrupted while sleeping...");
            }
        } catch (SQLException sqle) {
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

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    public static boolean setCurrentDirectory(String directory_name) {
        boolean result = false;  // Boolean indicating whether directory was set
        File directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs()) {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }

    // int main enables testing function from cmd line
    public static void main(String[] args) {
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
        ProjectQuery lt = new ProjectQuery();

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
        try {
            Thread.sleep(10);
        } catch (InterruptedException ie) {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }

}
