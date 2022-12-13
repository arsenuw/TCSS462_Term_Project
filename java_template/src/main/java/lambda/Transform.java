package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

import java.util.*;
import java.io.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {

    String readBucketName = "";
    String writeBucketName = "";

    String readFilename = "";
    String writeFilename = "";
    // Collect inital data.
    Inspector inspector = new Inspector();

    String outputCSV = "output.csv";

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        
        inspector.inspectContainer();

        // ****************START FUNCTION IMPLEMENTATION*************************
        // Add custom key/value attribute to SAAF's output. (OPTIONAL)
        inspector.addAttribute("message", "Hello " + request.getName()
                + "! This is an attributed added to the Inspector!");

        //NEED A READ BUCKETNAME
        readBucketName = request.getReadBucketName();

        writeBucketName = request.getWriteBucketName();
        
        readFilename = request.getReadilename();
        // need a read file name and a write file name
        writeFilename = request.getWriteFilename();

        //String inFile = "X:/home/arsen/tcss462_term_project/SAAF/java_template/SalesRecords.csv";
        //String outFile "X:/home/arsen/tcss462_term_project/SAAF/java_template/out.csv";
        //updateCSV(writeFilename);

        /*
         * Reading from s3 bucket
         */
        //Creates new file on S3 bucket
        AmazonS3 s3ClientRead = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3ClientRead.getObject(new GetObjectRequest(readBucketName, readFilename));

        //get content of the file
        InputStreamReader objectData = new InputStreamReader(s3Object.getObjectContent());

        //Call updateCSV - TRANSFORM CSV
        updateCSV(objectData);

        /*
         * Write to s3 bucket
         */
        Scanner sc;
        byte[] bytes = null;
        sc = new Scanner(outputCSV);
        sc.useDelimiter("\n");
        StringWriter file = new StringWriter();
        while (sc.hasNext()) {
            file.append(sc.next());
        }
        bytes = file.toString().getBytes(StandardCharsets.UTF_8);
        System.out.print(file);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        // Create new file on S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(writeBucketName, writeFilename, inputStream, meta);
        //Collect inital data.

        sc.close();
        Response response = new Response();
        //response.setB(writeBucketName);
        //response.setF(writeFilename);
    
 
         // Create and populate a separate response object for function output.
         // (OPTIONAL)
         response.setValue("Bucket: " + writeBucketName + " filename:" + writeFilename + " processed.");
         inspector.consumeResponse(response);

        

        // ****************END FUNCTION IMPLEMENTATION***************************

        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    /**
     * Transforming the CSV file
     */
    private Map<String, String> ordPriorityMap = Map.of(
            "L", "Low",
            "M", "Medium",
            "H", "High",
            "C", "Critical");
    private final String totRevenue = "Total Revenue";
    private final String totProfit = "Total Profit";
    private final String orderId = "Order ID";
    private final String ordPriority = "Order Priority";
    private final String grossMargin = "Gross Margin";
    private int ordPriorityCol;
    private int totRevenueCol;
    private int totProfitCol;
    private int orderIdCol;

    /* 
    public static void main(String args[]) { 
        String inFile = "X:/home/arsen/tcss462_term_project/100 Sales Records.csv";
        String outFile = "/home/arsen/tcss462_term_project/out.csv";
        updateCSV(inFile, outFile);
   }
    */

    //parse CSV column name line at get the index for columns
    private void ParseIndex(String line) {
        String[] row = line.trim().split("\\s*,\\s*");
        for(int i = 0; i < row.length; i ++){
            System.out.println(row[i]);
            switch(row[i]){
                case totRevenue:
                    totRevenueCol = i;
                    break;
                case totProfit:
                    totProfitCol = i;
                    break;
                case orderId:
                    orderIdCol = i;
                    break;
                case ordPriority:
                    ordPriorityCol = i;
                    break;
                default:
                    break;
            }
        }
    }

    public void updateCSV(InputStreamReader objectData) {

        //Do the transformations to the csv file
        String inString = "";
        HashSet<String> orders = new HashSet<>(); // Store all order IDs
        final String lineSep=System.getProperty("line.separator");
        try {
            BufferedReader reader = new BufferedReader(objectData);
            
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputCSV)));
            int i = 0;
            while ((inString = reader.readLine()) != null) {
                if(i == 0){
                    ParseIndex(inString);
                    inString = inString + "," + grossMargin;
                    writer.write(inString + lineSep); // add gross margin column
                    i++;
                    continue;
                }
                String[] row = inString.trim().split("\\s*,\\s*"); // parse line to row with columns separated by comma and remove spaces

                //transformation #2
                //convert order priority
                row[ordPriorityCol] = ordPriorityMap.get(row[ordPriorityCol]);

                //transformation #3
                // compute gross margin with total profit divided by total revenue
                Float grossMargin = Float.parseFloat(row[totProfitCol]) / Float.parseFloat(row[totRevenueCol]);

                //transformation #4
                String orderID = row[orderIdCol];
                //if order id is contained in the orders hashset then there is a duplicate, thus ignore this order; else add this order id into hashset
                if(orders.contains(orderID)){
                    continue;
                }else{
                    orders.add(orderID);
                }

                // write the converted line into new csv
                inString = String.join(",", row) + "," + grossMargin; // add gross margin to the line
                writer.write(inString + lineSep);
                i++;
            }
            writer.close();
        }catch (FileNotFoundException ex) {
            System.out.println("File Not Found");
        } catch (IOException ex) {
            System.out.println("File Error");
        }

        //filename = outFile;
        

    }

}
