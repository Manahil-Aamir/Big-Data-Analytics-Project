import os

def split_file(file_path, chunk_size=102400):
    """
    Splits the given file into smaller chunks, keeping the header in all chunks.

    :param file_path: Path to the file to be split
    :param chunk_size: Size of each chunk in bytes (default is 100 KB)
    """
    # Create a directory to store chunks
    output_dir = f"{os.path.splitext(file_path)[0]}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Read and split the file
    with open(file_path, 'rb') as file:
        # Read the header
        header = file.readline()
        chunk_number = 0
        while True:
            chunk = file.read(chunk_size - len(header))
            if not chunk:
                break
            chunk_file_path = os.path.join(output_dir, f"part_{chunk_number}")
            with open(chunk_file_path, 'wb') as chunk_file:
                chunk_file.write(header + chunk)
            chunk_number += 1

    print(f"File '{file_path}' split into {chunk_number} parts in directory '{output_dir}'.")

if __name__ == "__main__":
    # Ask user for the file to split
    file_path = input("Enter the path of the file to be split: ").strip()
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
    else:
        chunk_size = input("Enter chunk size in bytes (default is 102400): ").strip()
        try:
            chunk_size = int(chunk_size) if chunk_size else 102400
            split_file(file_path, chunk_size)
        except ValueError:
            print("Invalid chunk size. Please enter a numeric value.")
