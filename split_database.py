import os
import sqlite3

def split_database(db_file, chunk_size=10 * 1024 * 1024):  # 10MB chunks
    """
    Split a large SQLite database file into smaller chunks.

    :param db_file: Path to the SQLite database file
    :param chunk_size: Size of each chunk in bytes (default: 10MB)
    """
    # Open the database file in binary mode
    with open(db_file, "rb") as f:
        # Read the file contents
        data = f.read()

        # Calculate the number of chunks
        num_chunks = (len(data) + chunk_size - 1) // chunk_size

        # Create a folder to store the chunks
        chunk_folder = os.path.splitext(db_file)[0] + "_parts"
        if not os.path.exists(chunk_folder):
            os.makedirs(chunk_folder)

        # Split the data into chunks
        for i in range(num_chunks):
            # Calculate the start and end indices for this chunk
            start = i * chunk_size
            end = min((i + 1) * chunk_size, len(data))

            # Write the chunk to a file
            chunk_file = os.path.join(chunk_folder, f"database.part{i+1}")
            with open(chunk_file, "wb") as chunk:
                chunk.write(data[start:end])

    print(f"Database split into chunks in {chunk_folder}")

# Usage
db_file = "nhentai_db.sqlite"
split_database(db_file)