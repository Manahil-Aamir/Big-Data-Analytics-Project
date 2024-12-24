import kagglehub
import os
import shutil

# Desired output directory
output_dir = r"D:/Docker/Hadoop/Big-Data-Analytics-Project/data"

# Download dataset
dataset_path = kagglehub.dataset_download("bytadit/transactional-ecommerce")

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Extract and move only CSV files to the output directory
for root, dirs, files in os.walk(dataset_path):
    for file in files:
        if file.endswith(".csv"):
            # Construct full file paths
            full_file_path = os.path.join(root, file)
            target_path = os.path.join(output_dir, file)
            # Copy CSV files to the desired directory
            shutil.copy(full_file_path, target_path)

print("CSV files extracted to:", output_dir)
