import os
import pandas as pd
import json

def infer_avro_type(dtype):
    """Infer the Avro data type from pandas dtype."""
    if pd.api.types.is_string_dtype(dtype):
        return "string"
    elif pd.api.types.is_numeric_dtype(dtype):
        return "double"
    elif pd.api.types.is_integer_dtype(dtype):
        return "int"
    elif pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    else:
        return "string"  # Default type

def generate_avro_schema(csv_file_path, schema_name, schema_output_path):
    """
    Generate Avro schema dynamically from a CSV file.
    """
    if not os.path.exists(csv_file_path):
        print(f"CSV file not found: {csv_file_path}")
        return False
    
    try:
        df = pd.read_csv(csv_file_path)

        if df.empty:
            print(f"CSV file is empty: {csv_file_path}")
            return False

        # Infer schema from the dataframe
        fields = []
        for column in df.columns:
            avro_type = infer_avro_type(df[column].dtype)
            fields.append({"name": column, "type": avro_type})

        # Create the Avro schema
        avro_schema = {
            "type": "record",
            "name": schema_name,
            "fields": fields
        }

        # Write schema to output path
        with open(schema_output_path, "w") as schema_file:
            json.dump(avro_schema, schema_file, indent=4)
        print(f"Schema successfully generated: {schema_output_path}")
        return True
    except Exception as e:
        print(f"Error generating schema: {e}")
        return False

def generate_schemas_for_csvs(csv_dir):
    # Define the CSV to Avro schema file mapping
    csv_files = {
        "click_stream.csv": "click_stream_schema.avsc",
        "customer.csv": "customer_schema.avsc",
        "product.csv": "product_schema.avsc",
        "transactions.csv": "transactions_schema.avsc"
    }

    # Ensure the "schemas" directory exists
    schemas_dir = os.path.join(csv_dir, "schemas")
    if not os.path.exists(schemas_dir):
        os.makedirs(schemas_dir)

    # Generate Avro schema files for each CSV
    for csv_file, schema_file in csv_files.items():
        csv_file_path = os.path.join(csv_dir, csv_file)
        schema_output_path = os.path.join(schemas_dir, schema_file)
        schema_name = os.path.splitext(csv_file)[0] + "_schema"
        
        # Generate schema for the current CSV file
        generate_avro_schema(csv_file_path, schema_name, schema_output_path)

# Main execution
if __name__ == "__main__":
    csv_dir = input("Please enter the directory path where your CSV files are located: ")

    if not os.path.isdir(csv_dir):
        print(f"The directory {csv_dir} does not exist. Please provide a valid directory path.")
        exit(1)

    # Generate schemas for the provided CSV files
    generate_schemas_for_csvs(csv_dir)
