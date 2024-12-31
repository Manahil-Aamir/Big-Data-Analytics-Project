import pandas as pd

# Load the CSV file
file_path = 'Data/click_stream_reduced.csv'
df = pd.read_csv(file_path)

# Count the number of empty values in each column
empty_counts = df.isnull().sum()

# Print the results
print("Number of empty values in each column:")
print(empty_counts)