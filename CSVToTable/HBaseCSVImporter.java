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

public class HBaseCSVImporter {
    public static void main(String[] args) {
        String csvFile = "/root/customer.csv"; // Path to your CSV file
        String tableName = "customer"; // HBase table name
        int batchSize = 5000; // Adjust batch size for optimal performance

        try {
            // Step 1: Set up HBase configuration
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "172.30.208.1");
            config.set("hbase.zookeeper.property.clientPort", "2181");

            // Step 2: Establish a connection to HBase
            try (Connection connection = ConnectionFactory.createConnection(config);
                 Table table = connection.getTable(TableName.valueOf(tableName));
                 BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

                // Step 3: Read and parse the CSV file manually
                String headerLine = br.readLine(); // Read and ignore the header row
                String line;
                List<Put> puts = new ArrayList<>();
                int rowNumber = 0;

                while ((line = br.readLine()) != null) {
                    try {
                        // Split the line into columns
                        String[] values = line.split(","); // Assumes no embedded commas in fields
                        if (values.length < 15) {
                            System.err.println("Skipping invalid row #" + (rowNumber + 1) + ": " + line);
                            continue;
                        }

                        // Step 4: Create a Put operation for the row
                        String rowKey = values[0]; // Assuming 'customer_id' is the first column
                        Put put = new Put(Bytes.toBytes(rowKey));

                        // Add columns for 'personal_info'
                        put.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("first_name"), Bytes.toBytes(values[1]));
                        put.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("last_name"), Bytes.toBytes(values[2]));
                        put.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("username"), Bytes.toBytes(values[3]));
                        put.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("email"), Bytes.toBytes(values[4]));
                        put.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("gender"), Bytes.toBytes(values[5]));
                        put.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("birthdate"), Bytes.toBytes(values[6]));

                        // Add columns for 'device_info'
                        put.addColumn(Bytes.toBytes("device_info"), Bytes.toBytes("device_type"), Bytes.toBytes(values[7]));
                        put.addColumn(Bytes.toBytes("device_info"), Bytes.toBytes("device_id"), Bytes.toBytes(values[8]));
                        put.addColumn(Bytes.toBytes("device_info"), Bytes.toBytes("device_version"), Bytes.toBytes(values[9]));

                        // Add columns for 'location_info'
                        put.addColumn(Bytes.toBytes("location_info"), Bytes.toBytes("home_location_lat"), Bytes.toBytes(values[10]));
                        put.addColumn(Bytes.toBytes("location_info"), Bytes.toBytes("home_location_long"), Bytes.toBytes(values[11]));
                        put.addColumn(Bytes.toBytes("location_info"), Bytes.toBytes("home_location"), Bytes.toBytes(values[12]));
                        put.addColumn(Bytes.toBytes("location_info"), Bytes.toBytes("home_country"), Bytes.toBytes(values[13]));

                        // Add columns for 'join_info'
                        put.addColumn(Bytes.toBytes("join_info"), Bytes.toBytes("first_join_date"), Bytes.toBytes(values[14]));

                        puts.add(put);
                        rowNumber++;

                        // Insert batch into HBase if batch size is reached
                        if (puts.size() >= batchSize) {
                            table.put(puts);
                            puts.clear(); // Clear the list for the next batch
                            System.out.println("Inserted " + rowNumber + " rows so far...");
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing row #" + rowNumber + ": " + line);
                        e.printStackTrace();
                    }
                }

                // Insert any remaining rows
                if (!puts.isEmpty()) {
                    table.put(puts);
                    System.out.println("Inserted remaining rows. Total rows: " + rowNumber);
                }

                System.out.println("Data successfully inserted into HBase.");
            } catch (Exception e) {
                System.err.println("Error connecting to HBase or processing data.");
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("Error initializing HBase configuration or CSV reader.");
            e.printStackTrace();
        }
    }
}

