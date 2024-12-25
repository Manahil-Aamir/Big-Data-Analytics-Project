import os
import pandas as pd

# Function to calculate the scaling factor based on current and target sizes
def calculate_scaling_factor(file_size, total_size, target_size_gb):
    target_size_bytes = target_size_gb * (1024**3)  # Convert GB to bytes
    scaling_factor = (target_size_bytes * (file_size / total_size)) / file_size
    return scaling_factor

# Function to read a CSV, scale the number of rows based on the scaling factor, and save to the original file
def scale_csv(file_path, scaling_factor):
    try:
        # Try reading the CSV file while skipping bad lines (inconsistent number of columns)
        df = pd.read_csv(file_path, on_bad_lines='skip')  # Skip bad lines with inconsistent columns
        total_rows = len(df)
        scaled_rows = int(total_rows * scaling_factor)
        
        # If the scaled row count exceeds the total rows, repeat the data
        if scaled_rows > total_rows:
            df_scaled = pd.concat([df] * (scaled_rows // total_rows) + [df.head(scaled_rows % total_rows)], ignore_index=True)
        else:
            df_scaled = df.head(scaled_rows)
        
        # Overwrite the original CSV with the scaled data
        df_scaled.to_csv(file_path, index=False)
        print(f"Scaled and saved {file_path}")
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Get the directory of the current .py file
current_dir = os.path.dirname(os.path.abspath(__file__))

# List of CSV files to scale
csv_files = ['product.csv', 'customer.csv', 'transactions.csv', 'click_stream.csv']
csv_files = [os.path.join(current_dir, file) for file in csv_files]

target_size_gb = 3  # Target total size in GB

# Calculate current total size of the CSV files
current_total_size = sum(os.path.getsize(file) for file in csv_files)

# Process each file individually and scale it according to its size
for file in csv_files:
    current_file_size = os.path.getsize(file)
    
    # Calculate the scaling factor based on the current file size, keeping the ratio proportional
    scaling_factor = calculate_scaling_factor(current_file_size, current_total_size, target_size_gb)
    
    # Scale each CSV file and overwrite the original file
    scale_csv(file, scaling_factor)
