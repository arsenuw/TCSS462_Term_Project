package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import saaf.Inspector;
import saaf.Response;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public class ReadAndWriteCSV implements RequestHandler<Request, HashMap<String, Object>> {

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

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        Inspector inspector = new Inspector();
        inspector.inspectAll();

        String readBucketName = request.getBucketname();
        String readFilename = request.getFilename();
        String transformName = request.getTransformName();
        
//        String transformFileName = r

        //Creates new file on S3 bucket
        AmazonS3 s3ClientRead = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3ClientRead.getObject(new GetObjectRequest(readBucketName, readFilename));

        //get content of the file
        InputStreamReader objectData = new InputStreamReader(s3Object.getObjectContent());

        //transform function
        updateCSV(objectData, readBucketName, transformName);

        Response response = new Response(); 
        //response.set 
        response.setValue("transform");
       // response.setBucket(readBucketName);
      //  response.setName(readFilename);

        inspector.consumeResponse(response);
        return inspector.finish();

    }

    //parse CSV column name line at get the index for columns
    private void ParseIndex(String line) {
        String[] row = line.trim().split("\\s*,\\s*");
        for (int i = 0; i < row.length; i++) {
            System.out.println(row[i]);
            switch (row[i]) {
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

    public void updateCSV(InputStreamReader objectData, String bucketname, String transformName) {

        //Do the transformations to the csv file
        String inString = "";
        HashSet<String> orders = new HashSet<>(); // Store all order IDs
        final String lineSep = System.getProperty("line.separator");
        try {
            BufferedReader reader = new BufferedReader(objectData);
            StringWriter transformFile = new StringWriter();
            int i = 0;

            while ((inString = reader.readLine()) != null) {
                if (i == 0) {
                    ParseIndex(inString);
                    inString = inString + "," + grossMargin;
                    transformFile.write(inString + lineSep); // add gross margin column
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
                if (orders.contains(orderID)) {
                    continue;
                } else {
                    orders.add(orderID);
                }

                // write the converted line into new csv
                inString = String.join(",", row) + "," + grossMargin; // add gross margin to the line
                transformFile.write(inString + lineSep);
                i++;
            }
            transformFile.close();
            
            byte[] bytes = transformFile.toString().getBytes(StandardCharsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(bytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain");

            // Create new file on S3
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            s3Client.putObject(bucketname, transformName, inputStream, meta);

        } catch (FileNotFoundException ex) {
            System.out.println("File Not Found");
        } catch (IOException ex) {
            System.out.println("File Error");
        }

        //filename = outFile;
    }
}
