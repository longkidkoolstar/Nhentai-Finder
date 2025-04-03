import os

def reassemble_database(chunk_folder, db_file):
    """
    Reassemble a SQLite database from chunked data.

    :param chunk_folder: Path to the folder containing the chunked data
    :param db_file: Path to the output SQLite database file
    """
    # Get the absolute path to the chunk folder
    chunk_folder = os.path.join(os.getcwd(), chunk_folder)

    # Get all chunk files sorted by number
    chunk_files = sorted(
        [f for f in os.listdir(chunk_folder) if f.startswith("database.part")],
        key=lambda x: int(x.split("part")[-1])
    )

    # Merge all chunks
    with open(db_file, "wb") as merged:
        for chunk_file in chunk_files:
            with open(os.path.join(chunk_folder, chunk_file), "rb") as chunk:
                merged.write(chunk.read())

    print(f"Database reassembled as {db_file}")

# Usage
chunk_folder = "nhentai_db_parts"
db_file = "reassembled_nhentai_db.sqlite"
reassemble_database(chunk_folder, db_file)