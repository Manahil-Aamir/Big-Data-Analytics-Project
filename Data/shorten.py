import pandas as pd

# Load the CSV file
file_path = 'D:/Docker/Hadoop/Big-Data-Analytics-Project/Data/click_stream.csv'
df = pd.read_csv(file_path)

# Calculate the number of rows to keep to reduce the file size to 400MB
target_size_mb = 400
current_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
rows_to_keep = int(len(df) * (target_size_mb / current_size_mb))

# Reduce the DataFrame size
df_reduced = df.sample(n=rows_to_keep, random_state=1)

# Save the reduced DataFrame to a new CSV file
output_path = 'D:/Docker/Hadoop/Big-Data-Analytics-Project/Data/click_stream_reduced.csv'
df_reduced.to_csv(output_path, index=False)

print(f"Reduced file saved to {output_path}")