package lambda;

//import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

//import java.io.ByteArrayInputStream;
import java.io.InputStream;
//import java.io.StringWriter;
//import java.nio.charset.StandardCharsets;
import saaf.Inspector;
import saaf.Response;
import java.util.HashMap;
//import java.util.Random;
import java.util.Scanner;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        //Collect inital data. 
   
    
        
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)   
        inspector.addAttribute("message", "Hello " + request.getName() 
        + "! This is an attributed added to the Inspector!");
        long total; 
        long elements;  
        double avg;  
        //String  bucketname = request.getBucketname(); 
        //String filename = request.getFilename();
        
        //  int row = request.getRow(); 
      //  int col = request.getCol(); 
        String bucketname = request.getBucketname(); 
        String filename = request.getFilename();  
        
      //  int val = 0;
   // StringWriter sw = new StringWriter();
   // Random rand = new Random();
    
      //  byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
       // InputStream is = new ByteArrayInputStream(bytes);
       // ObjectMetadata meta = new ObjectMetadata();
       // meta.setContentLength(bytes.length);
       // meta.setContentType("text/plain");  
// Create new file on S3
       // AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
//s3Client.putObject(bucketname, filename, is, meta); 
        
total = 0;
elements = 0; 
avg = 0; 
String [] Tokens;
//try {  
    String srcBucket = bucketname; 
    String srcKey = filename; 
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build(); 
//get object file using source bucket and srcKey name
S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
//get content of the file
InputStream objectData = s3Object.getObjectContent();
//scanning data line by line
String text = "";
Scanner scanner = new Scanner(objectData); 
while (scanner.hasNext()) {  
//text += scanner.nextLine();
text += scanner.nextLine();
text +=",";
elements=elements+1; 
}  
Tokens = text.split(",");     
String [] arr = Tokens;
for(int i =0; i<arr.length; i++) {  
    long temp=0;
    try{  
        temp= Long.parseLong(arr[i]);
        total =total+temp;
    }catch(NumberFormatException e) { 

    }
}  

avg = total/elements;
scanner.close(); 
//} catch (AmazonServiceException e) { 
//    e.printStackTrace(); 
    
//}
        
        LambdaLogger logger = context.getLogger(); 
        logger.log("ProcessCSV bucketname:"+bucketname+" filename:"+filename+" avg:"+avg+" total:"+total+" elements:"+elements);
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response(); 
      //  response.setValue("Bucket:" + bucketname + " filename:" + filename + " size:" +
//bytes.length+" total:"+total+" elements:"+elements+" avg:"+avg); 
        response.setValue("Bucket: "+bucketname+" filename:"+filename+ " processed."); 
     
        //response.setValue("Hello " + request.getNameALLCAPS()
             //   + "! This is from a response object!");
        
        inspector.consumeResponse(response);
          //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
