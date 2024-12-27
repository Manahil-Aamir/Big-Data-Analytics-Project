import os

# Ask the user for the directory containing the files
directory = input("Please enter the directory containing the CSV files: ")

# List of source files
source_files = ["customer.csv", "product.csv", "transactions.csv"]  # Replace with your .csv file names
target_size_mb = 500  # Desired size in MB

# Convert target size to bytes
target_size_bytes = target_size_mb * 1024 * 1024

# Function to oversample a file
def oversample_file(source_file, target_size_bytes):
    if not os.path.exists(source_file):
        print(f"Source file '{source_file}' not found!")
        return

    source_size_bytes = os.path.getsize(source_file)
    if source_size_bytes == 0:
        print(f"Source file '{source_file}' is empty!")
        return

    copies_needed = target_size_bytes // source_size_bytes
    remainder = target_size_bytes % source_size_bytes

    with open(source_file, "r+") as outfile:  # Open the same file for overwriting
        content = outfile.read()
        outfile.seek(0)  # Reset pointer to the beginning

        # Write full copies of the source file
        for _ in range(copies_needed):
            outfile.write(content)

        # Write the remaining portion to match exact size
        if remainder > 0:
            lines = content.splitlines(keepends=True)
            written_bytes = 0
            for line in lines:
                if written_bytes + len(line.encode()) > remainder:
                    break
                outfile.write(line)
                written_bytes += len(line.encode())

    print(f"File '{source_file}' has been oversampled to {target_size_mb} MB!")

# Process each file in the list
for source_file in source_files:
    full_path = os.path.join(directory, source_file)
    oversample_file(full_path, target_size_bytes)
