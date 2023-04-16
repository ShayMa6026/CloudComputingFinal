package lambda.service3;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mysql.cj.util.StringUtils;
import saaf.Inspector;

import java.sql.*;
import java.util.HashMap;


public class Query implements RequestHandler<Request, HashMap<String, Object>> {


    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        LambdaLogger logger = context.getLogger();

        logger.log("[Query] handleRequest");
        //****************START FUNCTION IMPLEMENTATION*************************

        logger.log("[Query] request: " + request.toString());
        HashMap<String, Object> result = new HashMap<>();

        // Connect to database
        try {
            Connection connection = connectDatabase();
            Statement statement = connection.createStatement();

            process(inspector, statement, request, result, logger);

            Response response = new Response();
            response.setResult(result);
            inspector.addAttribute("result", response);

            statement.close();
            connection.close();
        } catch (SQLException e) {
            inspector.addAttribute("error", "Error while execute SQL");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }

        return inspector.finish();


        //****************END FUNCTION IMPLEMENTATION***************************

    }


    public Connection connectDatabase() {
        Connection conn;

        String url = "jdbc:mysql://tcss562-team20-project.cp1twaxumawq.us-east-2.rds.amazonaws.com:3306/tcss562proj";
        String username = "admin";
        String password = "12345678";

        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return conn;

    }


    public void process(Inspector inspector, Statement statement, Request request, HashMap<String, Object> result, LambdaLogger logger) {
        String aggregateQuery = generateSQL(request, logger);

        // Execute and store result data
        try {
            ResultSet queryResult = statement.executeQuery(aggregateQuery);
            while (queryResult.next()) {
                result.put("Avg Order Processing Time", queryResult.getDouble("Avg Order Processing Time"));
                result.put("Avg Gross Margin", queryResult.getDouble("Avg Gross Margin"));
                result.put("Avg Units Sold", queryResult.getDouble("Avg Units Sold"));
                result.put("Max Units Sold", queryResult.getDouble("Max Units Sold"));
                result.put("Min Units Sold", queryResult.getDouble("Min Units Sold"));
                result.put("Sum Units Sold", queryResult.getDouble("Sum Units Sold"));
                result.put("Sum Total Revenue", queryResult.getDouble("Sum Total Revenue"));
                result.put("Sum Total Profit", queryResult.getDouble("Sum Total Profit"));
                result.put("Total Orders", queryResult.getDouble("Total Orders"));
            }
        } catch (SQLException e) {
            inspector.addAttribute("error", "Error while aggregating SQL from database");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }
    }


    private String generateSQL(Request request, LambdaLogger logger) {
        StringBuilder sb = new StringBuilder();
        String tableName = "sales_records";
        String base = "SELECT AVG(sr.`Order Processing Time`) as `Avg Order Processing Time`," + "AVG(sr.`Gross Margin`) as `Avg Gross Margin`," + "AVG(sr.`Units Sold`) as `Avg Units Sold`, " + "MAX(sr.`Units Sold`) as `Max Units Sold`, " + "MIN(sr.`Units Sold`) as `Min Units Sold`, " + "SUM(sr.`Units Sold`) as `Sum Units Sold`, " + "SUM(sr.`Total Revenue`) as `Sum Total Revenue`, " + "SUM(sr.`Total Profit`) as `Sum Total Profit`, " + "COUNT(*) as `Total Orders`" + " FROM " + tableName + " sr " + "where 1 = 1";

        sb.append(base);

        if (!StringUtils.isNullOrEmpty(request.getRegion())) {
            sb.append(" and ").append("Region = ").append("\"").append(request.getRegion()).append("\"");
        }
        if (!StringUtils.isNullOrEmpty(request.getCountry())) {
            sb.append(" and ").append("Country = ").append("\"").append(request.getCountry()).append("\"");
        }
        if (!StringUtils.isNullOrEmpty(request.getItemType())) {
            sb.append(" and ").append("`Item Type` = ").append("\"").append(request.getItemType()).append("\"");
        }
        if (!StringUtils.isNullOrEmpty(request.getOrderPriority())) {
            sb.append(" and ").append("`Order Priority` = ").append("\"").append(request.getOrderPriority()).append("\"");
        }
        if (!StringUtils.isNullOrEmpty(request.getSalesChannel())) {
            sb.append(" and ").append("`Sales Channel` = ").append("\"").append(request.getSalesChannel()).append("\"");
        }

        logger.log("SQL: " + sb);
        return sb.toString();
    }


}
