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

                        // Step 7: Handle missing or incomplete rows by assigning default values
                        String sessionId = values.length > 0 ? values[0] : "UNKNOWN_SESSION";
                        String eventName = values.length > 1 ? values[1] : "UNKNOWN_EVENT";
                        String eventTime = values.length > 2 ? values[2] : "1970-01-01T00:00:00Z";
                        String eventId = values.length > 3 ? values[3] : "UNKNOWN_EVENT_ID";
                        String trafficSource = values.length > 4 ? values[4] : "UNKNOWN_SOURCE";
                        String eventMetadata = values.length > 5 && !values[5].isEmpty() ? values[5] : "event";

                        // Step 8: Create a new Put operation for this row using the session ID as the row key
                        Put put = new Put(Bytes.toBytes(sessionId));

                        // Step 9: Add columns for the 'event_details' column family
                        put.addColumn(Bytes.toBytes("event_details"), Bytes.toBytes("event_name"), Bytes.toBytes(eventName));
                        put.addColumn(Bytes.toBytes("event_details"), Bytes.toBytes("event_time"), Bytes.toBytes(eventTime));
                        put.addColumn(Bytes.toBytes("event_details"), Bytes.toBytes("event_id"), Bytes.toBytes(eventId));

                        // Step 10: Add columns for the 'traffic_info' column family
                        put.addColumn(Bytes.toBytes("traffic_info"), Bytes.toBytes("traffic_source"), Bytes.toBytes(trafficSource));

                        // Step 11: Add columns for the 'metadata' column family
                        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("event_metadata"), Bytes.toBytes(eventMetadata));

                        // Step 12: Add this Put operation to the batch list
                        puts.add(put);

                        // Step 13: If batch size reaches 1000, insert the batch into HBase
                        if (puts.size() >= 10000) {
                            System.out.println("Inserting batch of 10000 rows."); // Debug: Notify about batch insertion
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
