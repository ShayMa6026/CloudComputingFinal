package lambda.service1;

import java.io.*;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.time.LocalDate;
import static java.time.temporal.ChronoUnit.DAYS;


public class TransfromLocalTest {
    public static void main(String[] args) throws FileNotFoundException {

        Scanner scanner = new Scanner(new File("java_template/LocalTestData/100SalesRecords2.csv"));
        List<ArrayList<String>> rows = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String row = scanner.nextLine();
            String[] rowArray = row.split(",");
            ArrayList<String> rowList = new ArrayList<String>(Arrays.asList(rowArray));
            rows.add(rowList);
        }

        // add processing time column
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
                rows.get(i).add(String.valueOf(processingTime)); //to be converted to integer when creating new csv
                processingTime = DAYS.between(orderDateParsed, shipDateParsed);
            }
        }

        //Add [Gross Margin]
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
                rows.get(i).add(String.format("%.2f", grossMargin)); //to be converted to double when creating csv
            }
        }

        //Add [Order Priority]column
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


            // remove duplicates
            HashSet<String> set = new HashSet<>();
            int orderIDIndex = rows.get(0).indexOf("Order ID");
            for (int i = 1; i < rows.size(); i++) {
                if (set.contains(rows.get(i).get(orderIDIndex))) {
                    rows.remove(i);
                } else {
                    set.add(rows.get(i).get(orderIDIndex));
                }
            }


            for (int i = 0; i < rows.size(); i++) {
                System.out.println(i + ":" + rows.get(i).toString());
            }

        }
    }

