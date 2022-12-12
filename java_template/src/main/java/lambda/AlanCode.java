package lambda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class AlanCode {
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

//    public static void main(String args[]){
//        String inFile = "X:\\GitHub\\tst.csv";
//        String outFile = "X:\\GitHub\\out.csv";
//        updateCSV(inFile, outFile);
//    }

    //parse CSV column name line at get the index for columns
    private void ParseIndex(String line){
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

    public void updateCSV(String inFile, String outFile) {
        String inString = "";
        HashSet<String> orders = new HashSet<>(); // Store all order IDs
        final String lineSep=System.getProperty("line.separator");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(inFile)));
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outFile)));
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

    }
}
