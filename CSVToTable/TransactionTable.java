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

public class TransactionTable {
    public static void main(String[] args) {
        // Path to the CSV file and HBase table name
        String csvFile = "/root/transactions.csv"; // Update the path to your CSV file
        String tableName = "transactions"; // HBase table name

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
                    int rowNumber = 1;  // Row counter for debugging purposes

                    while ((line = br.readLine()) != null) {
                        // Step 6: Read each row of data (split by comma-separated values)
                        String[] values = line.split(",", -1); // Use -1 to retain empty strings
                        if (values.length < 14) {
                            System.err.println("Skipping invalid row #" + rowNumber + ": " + line); // Debug: Print skipped row
                            rowNumber++;
                            continue; // Skip invalid rows
                        }

                        // Step 7: Use 'booking_id' (third column) as the row key
                        String rowKey = values[2];

                        // Step 8: Create a new Put operation for this row
                        Put put = new Put(Bytes.toBytes(rowKey));

                        // Step 9: Add columns for the 'transaction_details' column family
                        put.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("created_at"), Bytes.toBytes(values[0]));
                        put.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("customer_id"), Bytes.toBytes(values[1]));
                        put.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("session_id"), Bytes.toBytes(values[3]));
                        put.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("product_metadata"), Bytes.toBytes(values[4]));

                        // Step 10: Add columns for the 'payment_info' column family
                        put.addColumn(Bytes.toBytes("payment_info"), Bytes.toBytes("payment_method"), Bytes.toBytes(values[5]));
                        put.addColumn(Bytes.toBytes("payment_info"), Bytes.toBytes("payment_status"), Bytes.toBytes(values[6]));
                        put.addColumn(Bytes.toBytes("payment_info"), Bytes.toBytes("promo_amount"), Bytes.toBytes(values[7]));
                        put.addColumn(Bytes.toBytes("payment_info"), Bytes.toBytes("promo_code"), Bytes.toBytes(values[8]));

                        // Step 11: Add columns for the 'shipment_info' column family
                        put.addColumn(Bytes.toBytes("shipment_info"), Bytes.toBytes("shipment_fee"), Bytes.toBytes(values[9]));
                        put.addColumn(Bytes.toBytes("shipment_info"), Bytes.toBytes("shipment_date_limit"), Bytes.toBytes(values[10]));
                        put.addColumn(Bytes.toBytes("shipment_info"), Bytes.toBytes("shipment_location_lat"), Bytes.toBytes(values[11]));
                        put.addColumn(Bytes.toBytes("shipment_info"), Bytes.toBytes("shipment_location_long"), Bytes.toBytes(values[12]));

                        // Step 12: Add columns for the 'transaction_summary' column family
                        put.addColumn(Bytes.toBytes("transaction_summary"), Bytes.toBytes("total_amount"), Bytes.toBytes(values[13]));

                        // Step 13: Add this Put operation to the batch list
                        puts.add(put);

                        // Step 14: If batch size reaches 1000, insert the batch into HBase
                        if (puts.size() >= 1000) {
                            System.out.println("Inserting batch of 1000 rows."); // Debug: Notify about batch insertion
                            table.put(puts);
                            puts.clear(); // Clear the list for the next batch
                        }

                        rowNumber++;
                    }

                    // Step 15: Insert any remaining rows in the batch
                    if (!puts.isEmpty()) {
                        System.out.println("Inserting remaining rows."); // Debug: Notify about final batch insertion
                        table.put(puts);
                    }

                    System.out.println("Data successfully inserted into HBase.");
                } catch (Exception e) {
                    // Step 16: Error handling for CSV reading
                    System.err.println("Error reading the CSV file.");
                    e.printStackTrace();
                }
            } catch (Exception e) {
                // Step 17: Error handling for HBase connection
                System.err.println("Error connecting to HBase.");
                e.printStackTrace();
            }
        } catch (Exception e) {
            // Step 18: Catch any other errors
            System.err.println("Error during HBase CSV import process.");
            e.printStackTrace();
        }
    }
}


