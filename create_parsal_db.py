import json
import os
import glob
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError

# Main function to create and populate the MongoDB database
def create_parsal_database(data_directory: str = 'archive_clean'):
    # 1. Connect to MongoDB 
    try:
        client = MongoClient('localhost', 27017)
        # Test the connection
        client.admin.command('ping')
        print("Connection to MongoDB successful.")
    except Exception as e:
        print(f"Unable to connect to MongoDB: {e}")
        return

    # 2. Select the database and collection
    db = client['parsal_db']
    articles_collection = db['articles']

    # 3. Check and scan the data folder ---
    if not os.path.isdir(data_directory):
        print(f"Error: The folder '{os.path.abspath(data_directory)}' was not found.")
        return

    # Find all .json files in the folder 
    json_files = glob.glob(os.path.join(data_directory, '*.json'))
    
    if not json_files:
        print(f"⚠️ No .json files found in folder '{data_directory}'.")
        return

    print(f"🔍 Found {len(json_files)} JSON files to import.")

    # 4. Load data in batch for
    articles_to_insert = []
    processed_files = 0
    failed_files = 0

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                article_data = json.load(f)
                # Ensure the data is not an empty list or other
                if isinstance(article_data, dict) and article_data:
                    articles_to_insert.append(article_data)
                else:
                    print(f"⚠️ Skipped file (invalid or empty format): {file_path}")
                    failed_files += 1

        except json.JSONDecodeError:
            print(f"❌ JSON decoding error in file: {file_path}")
            failed_files += 1
        except Exception as e:
            print(f"❌ Unexpected error with file {file_path}: {e}")
            failed_files += 1

    processed_files = len(articles_to_insert)

    if not articles_to_insert:
        print("No valid articles to insert.")
        return

    # 5. Insert data into the database
    print(f"\nInserting {len(articles_to_insert)} articles into the database...")
    try:
        # 'ordered=False' allows to continue even if there are errors (e.g. duplicates)
        result = articles_collection.insert_many(articles_to_insert, ordered=False)
        print(f"Inserted {len(result.inserted_ids)} new articles.")
    except BulkWriteError as bwe:
        write_errors = bwe.details.get('writeErrors', [])
        duplicate_errors = sum(1 for err in write_errors if err.get('code') == 11000)
        print(f"Completed with write errors. Inserted: {bwe.details.get('nInserted')}")
        if duplicate_errors > 0:
            print(f"   - {duplicate_errors} articles were already present (duplicates) and were skipped.")
    except Exception as e:
        print(f"Error during bulk insert: {e}")


    # 6. Create a unique id
    print("\nCreating a unique index on the 'doi' field...")
    try:
        articles_collection.create_index('doi', unique=True)
        print("Index created successfully!")
    except Exception as e:
        print(f"Error creating index: {e}")

    # --- Summary ---
    total_in_db = articles_collection.count_documents({})
    print("\n--- Summary ---")
    print(f"Processed files: {processed_files}")
    print(f"Skipped files: {failed_files}")
    print(f"Total articles on db: {total_in_db}")

if __name__ == "__main__":
    # We assume that the 'archive_clean' folder is in the same directory as this script.
    # Path: create_parsal_database('/path/to/archive_clean')
    create_parsal_database(data_directory='archive_clean')