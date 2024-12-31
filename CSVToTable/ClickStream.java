import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class ClickstreamToHBase {
    public static void main(String[] args) {
        // Path to the CSV file and HBase table name
        String csvFile = "/root/click_stream.csv"; // Update with the actual CSV file path
        String tableName = "click_stream"; // HBase table name

        try {
            // Step 1: Set up the HBase configuration
            Configuration config = HBaseConfiguration.create();

            // Step 2: Establish a connection to HBase
            try (Connection connection = ConnectionFactory.createConnection(config)) {
                Table table = connection.getTable(TableName.valueOf(tableName));

                // Step 3: Read the CSV file
                try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
                    String line;
                    // Step 4: Read the header row
                    String[] headers = br.readLine().split(","); // Assuming column names are separated by commas
                    System.out.println("CSV headers: " + String.join(", ", headers)); // Debug: Print CSV headers

                    // Step 5: Create a list to hold Put operations for batch insertion
                    List<Put> puts = new ArrayList<>();
                    int rowNumber = 1; // Row counter for debugging purposes

                    while ((line = br.readLine()) != null) {
                        // Step 6: Read each row of data (split by comma-separated values)
                        String[] values = line.split(",", -1); // Use -1 to retain empty strings
                        if (values.length < 6) {
                            System.err.println("Skipping invalid row #" + rowNumber + ": " + line); // Debug: Print skipped row
                            rowNumber++;
                            continue; // Skip invalid rows
                        }

                        // Step 7: Use 'session_id' (first column) as the row key
                        String rowKey = values[0];

                        // Step 8: Create a new Put operation for this row
                        Put put = new Put(Bytes.toBytes(rowKey));

                        // Step 9: Add columns for the 'event_details' column family
                        put.addColumn(Bytes.toBytes("event_details"), Bytes.toBytes("event_name"), Bytes.toBytes(values[1]));
                        put.addColumn(Bytes.toBytes("event_details"), Bytes.toBytes("event_time"), Bytes.toBytes(values[2]));
                        put.addColumn(Bytes.toBytes("event_details"), Bytes.toBytes("event_id"), Bytes.toBytes(values[3]));

                        // Step 10: Add columns for the 'traffic_info' column family
                        put.addColumn(Bytes.toBytes("traffic_info"), Bytes.toBytes("traffic_source"), Bytes.toBytes(values[4]));

                        // Step 11: Add columns for the 'metadata' column family
                        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("event_metadata"), Bytes.toBytes(values[5]));

                        // Step 12: Add this Put operation to the batch list
                        puts.add(put);

                        // Step 13: If batch size reaches 1000, insert the batch into HBase
                        if (puts.size() >= 10000) {
                            System.out.println("Inserting batch of 1000 rows."); // Debug: Notify about batch insertion
                            table.put(puts);
                            puts.clear(); // Clear the list for the next batch
                        }

                        rowNumber++;
                    }

                    // Step 14: Insert any remaining rows in the batch
                    if (!puts.isEmpty()) {
                        System.out.println("Inserting remaining rows."); // Debug: Notify about final batch insertion
                        table.put(puts);
                    }

                    System.out.println("Clickstream data successfully inserted into HBase.");
                } catch (Exception e) {
                    // Step 15: Error handling for CSV reading
                    System.err.println("Error reading the CSV file.");
                    e.printStackTrace();
                }
            } catch (Exception e) {
                // Step 16: Error handling for HBase connection
                System.err.println("Error connecting to HBase.");
                e.printStackTrace();
            }
        } catch (Exception e) {
            // Step 17: Catch any other errors
            System.err.println("Error during HBase CSV import process.");
            e.printStackTrace();
        }
    }
}
