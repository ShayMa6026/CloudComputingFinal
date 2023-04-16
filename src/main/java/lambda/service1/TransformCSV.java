package lambda.service1;

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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.time.LocalDate;
import static java.time.temporal.ChronoUnit.DAYS;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class TransformCSV implements RequestHandler<Request, HashMap<String, Object>> {
   String srcBucket = "";
   String srcKey = "";

    /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        srcBucket = request.getBucketname();
        srcKey = request.getFilename();

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get csv file using bucket and csv file name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
        //get file content
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line
        Scanner scanner = new Scanner(objectData);
        // arraylist to store rows of the csv file
        List<ArrayList<String>> rows = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String row = scanner.nextLine();
            String[] rowArray = row.split(",");
            ArrayList<String> rowList = new ArrayList<String>(Arrays.asList(rowArray));
            rows.add(rowList);
        }
        scanner.close();

        transformCSV(rows);

        LambdaLogger logger = context.getLogger();
        logger.log("TransformCSV bucketname:" + srcBucket + " filename:" + srcKey);

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: " + srcBucket + " filename:" + srcKey + " transformed.");
        inspector.consumeResponse(response);

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    public void transformCSV(List<ArrayList<String>> rows) {
        addProcessingTime(rows);
        orderPriorityTransform(rows);
        addGrossMargin(rows);
        removeDuplidates(rows);
        createTransformedCSV(rows);
    }

    //Add column [Order Processing Time] that stores an integer value representing the
    //number of days between the [Order Date] and [Ship Date]
    private void addProcessingTime(List<ArrayList<String>> rows) {
        //get index of order date and ship date
        int orderDateIndex = rows.get(0).indexOf("Order Date");
        int shipDateIndex = rows.get(0).indexOf("Ship Date");
        String orderDate = "";
        String shipDate = "";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
        long processingTime = -1l;

        for (int i = 0; i < rows.size(); i++) {
            if (i == 0) {
                rows.get(i).add("Order Processing Time");
            } else {
                orderDate = rows.get(i).get(orderDateIndex);
                shipDate = rows.get(i).get(shipDateIndex);
                LocalDate orderDateParsed = LocalDate.parse(orderDate, formatter);
                LocalDate shipDateParsed = LocalDate.parse(shipDate, formatter);
                processingTime = DAYS.between(orderDateParsed, shipDateParsed);
                rows.get(i).add(String.valueOf(processingTime)); //to be converted to int when creating csv
            }
        }
    }

    //Transform [Order Priority] column
    private void orderPriorityTransform(List<ArrayList<String>> rows) {
        int orderPriorityIndex = rows.get(0).indexOf("Order Priority");
        for (int i = 1; i < rows.size(); i++) {
            if (rows.get(i).get(orderPriorityIndex).equals("L")) {
                rows.get(i).set(orderPriorityIndex, "Low");
            }
            if (rows.get(i).get(orderPriorityIndex).equals("M")) {
                rows.get(i).set(orderPriorityIndex, "Medium");
            }
            if (rows.get(i).get(orderPriorityIndex).equals("H")) {
                rows.get(i).set(orderPriorityIndex, "High");
            }
            if (rows.get(i).get(orderPriorityIndex).equals("C")) {
                rows.get(i).set(orderPriorityIndex, "Critical");
            }
        }
    }

    //Add [Gross Margin]
    private void addGrossMargin(List<ArrayList<String>> rows) {
        int totalProfitIndex = rows.get(0).indexOf("Total Profit");
        int totalRevenueIndex = rows.get(0).indexOf("Total Revenue");
        double totalProfit = 0.0;
        double totalRevenue = 0.0;
        double grossMargin = 0.0;

        for (int i = 0; i < rows.size(); i++) {
            if (i == 0) {
                rows.get(i).add("Gross Margin");
            } else {
                totalProfit = Double.valueOf(rows.get(i).get(totalProfitIndex));
                totalRevenue = Double.valueOf(rows.get(i).get(totalRevenueIndex));
                grossMargin = totalProfit / totalRevenue;
                rows.get(i).add(String.format("%.2f", grossMargin)); //to be converted to float when creating csv
            }
        }
    }

    //Remove duplicate data identified by [Order ID]
    private void removeDuplidates(List<ArrayList<String>> rows) {
        HashSet<String> set = new HashSet<>();
        int orderIDIndex = rows.get(0).indexOf("Order ID");
        for (int i = 1; i < rows.size(); i++) {
            if(set.contains(rows.get(i).get(orderIDIndex))){
                rows.remove(i);
            }
            else {
                set.add(rows.get(i).get(orderIDIndex));
            }
            }
        }

    private void createTransformedCSV(List<ArrayList<String>> rows) {
        StringWriter stringWriter = new StringWriter();
        for(ArrayList<String> row : rows){
            for(int i = 0; i < row.size(); i++){
                stringWriter.append(row.get(i));
                if(i != row.size() - 1){
                    stringWriter.append(",");
                }
            }
            stringWriter.append("\n");
        }

        byte[] bytes = stringWriter.toString().getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(bytes.length);
        objectMetadata.setContentType("text/csv");

        // Create transformed csv file on S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        String transformedFileName =  "Transformed-" + srcKey;
        s3Client.putObject(srcBucket, transformedFileName, inputStream, objectMetadata);
    }
}








