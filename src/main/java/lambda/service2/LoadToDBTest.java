package lambda.service2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.sql.Date;

import com.amazonaws.services.lambda.runtime.*;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.io.InputStream;
import com.amazonaws.services.s3.model.S3Object;
import java.sql.SQLException;
import saaf.Response;
import java.util.Scanner;
import java.sql.DriverManager;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import saaf.Inspector;

import java.util.HashMap;


/**
 * lambda.service2::handleRequest
 *
 * @author Xiaojie Li
 */
public class LoadToDBTest implements RequestHandler<HashMap<String, String>, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     *
     * @param request Hashmap containing request JSON attributes.
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(HashMap<String, String> request, Context context) {
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************
        try {
            //obtaining S3 bucket attributes
            String bucketname = request.get("bucketname");
            String filename = request.get("filename");

            // get S3 object
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));	//get object file using source bucket and srcKey name
            InputStream objectData = s3Object.getObjectContent(); //get content of the file

            // database config
            String jdbcURL = "jdbc:mysql://tcss562-team20-project.cp1twaxumawq.us-east-2.rds.amazonaws.com:3306/tcss562proj";
            String username = "admin";
            String password = "12345678";

            // connect to database
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);;
            connection.setAutoCommit(false);

            // prepare sql
            String sql = "INSERT INTO sales_records (`Region`,`Country`,`Item Type`,`Sales Channel`,`Order Priority`," +
                    "`Order Date`,`Order ID`,`Ship Date`,`Units Sold`,`Unit Price`,`Unit Cost`,`Total Revenue`," +
                    "`Total Cost`,`Total Profit`,`Order Processing Time`,`Gross Margin`) VALUES " +
                    "(?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            Scanner scanner = new Scanner(objectData);	//scanning data line by line
            scanner.nextLine(); // skip the header
            int cnt = 0;

            while (scanner.hasNext()) {
                String record = scanner.nextLine();
                String[] data = record.split(",");
                for (int i = 0; i < 16; i++) {
                    if (i <= 4) {
                        statement.setString(i + 1, data[i]);
                    }
                    else if (i == 5 || i == 7) {
                        statement.setDate(i + 1, convertDate(data[i]));
                    } else if (i == 6 || i == 8) {
                        statement.setInt(i + 1, Integer.parseInt(data[i]));
                    } else {
                        statement.setFloat(i + 1, Float.parseFloat(data[i]));
                    }
                }

                statement.addBatch();
                statement.execute();
                cnt++;
            }
            // send log to CloudWatch logs
            LambdaLogger logger = context.getLogger();
            logger.log("Process to DB. bucketname:" + bucketname + " filename:" + filename
                    + " number of queries:" + String.valueOf(cnt) );

            scanner.close();
            connection.commit();
            connection.close();
            //Create and populate a separate response object for function output. (OPTIONAL)
            Response response = new Response();
            response.setValue("Bucket: " + bucketname + " filename:" + filename + " processed.");
            inspector.consumeResponse(response);
        } catch(SQLException ex) {
            ex.printStackTrace();
        }

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

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

        LoadToDBTest loadTest = new LoadToDBTest();

        // Create a request hash map
        HashMap req = new HashMap();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request hashmap
        req.put("name", name);

        req.put("bucketname", "tcss562-tmp-bucket");
        req.put("filename", "100_Sales_Records_no_dup.csv");
        // Report name to stdout
        System.out.println("cmd-line param name=" + req.get("name"));
        System.out.println("bucketname=" + req.get("bucketname"));
        System.out.println("filename=" + req.get("filename"));

        // Run the function
        HashMap resp = loadTest.handleRequest(req, c);

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }

    private Date convertDate(String s) {
        try {
            DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
            System.out.println(s);
            java.util.Date date = formatter.parse(s);
            SimpleDateFormat newFormat = new SimpleDateFormat("yyyy-MM-dd");
            String DateStr = newFormat.format(date);
            return Date.valueOf(DateStr);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
