import os
import hashlib
import argparse
from tinydb import TinyDB, Query
from shutil import move


def get_args():
    parser = argparse.ArgumentParser(description='Duplicate checker')
    parser.add_argument('source', default=os.getcwd(), help='Path to source dir, defaults to current directory')
    parser.add_argument('target', nargs='?', help='Path to target dir, defaults to source directory')
    parser.add_argument('-r', '--rebuild-inventory', action='store_true', help='Rebuilds inventory file')

    args = parser.parse_args()
    # Set target to the value of source if it's not provided.
    args.target = args.target if args.target else args.source

    return args


def chunk_reader(file, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        datachunk = file.read(chunk_size)
        if not datachunk:
            return
        yield datachunk


def gather_inventory(path):
    database = []
    duplicates = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            data = {'filename': filename}
            # Create an empty hashlib hash for filling
            sha_hash = hashlib.sha512()
            # Create the full filepath
            data['full_path'] = os.path.join(dirpath, filename)
            # Load up the files into the chunk reader
            with open(data['full_path'], 'rb') as file:
                for datachunk in chunk_reader(file):
                    sha_hash.update(datachunk)
            # digest that juicy file,
            data['hash'] = sha_hash.hexdigest()

            # Check if the current filehash exists in the database
            duplicate_found = False

            for entry in database:
                if data['hash'] == entry.get('hash'):
                    duplicate_found = True

            if duplicate_found:
                # Report and Delete the data
                print(f'Duplicate found: [{data["hash"][:7]}] {data["filename"]}')
                duplicates.append(data)
            else:
                # Store the collected data in the database
                database.append(data)
    return database, duplicates


def delete_duplicates(duplicates_list):
    for duplicate in duplicates_list:
        print(f'Deleting [{duplicate["hash"][:7]}] {duplicate["filename"]}')
        os.remove(duplicate['full_path'])


def build_inventory(inventory_filepath, data):
    with TinyDB(inventory_filepath) as db:
        for datum in data:
            db.insert(datum)


def stash(database, target_path, inventory_filepath):
    """Stores the database entries in the target path while checking hashes against the current inventory for
    duplicates."""
    print('Stashing!')
    inventory_filename = os.path.split(inventory_filepath)[-1]

    with TinyDB(inventory_filepath) as db:
        duplicates = []
        for file_entry in database:
            entry = Query()
            match = db.search(entry['hash'] == file_entry['hash'])
            if file_entry['filename'] == inventory_filename:
                print("Ignoring Inventory file")
                continue
            elif match:
                print(f'Duplicate found: [{file_entry["hash"][:7]}] {file_entry["filename"]}')
                duplicates.append(file_entry)
            else:
                # Move the file
                print('Moving:', file_entry['full_path'], '\n\tTo:', target_path)
                move(file_entry['full_path'], target_path)
                db.insert(file_entry)
        delete_duplicates(duplicates)


def main():
    args = get_args()
    target_path = args.target.replace('\\', '\\\\')
    source_path = args.source.replace('\\', '\\\\')
    inventory_filename = 'inventory.json'
    inventory_filepath = os.path.join(target_path, inventory_filename)

    if os.path.isfile(inventory_filepath) and args.rebuild_inventory:
        print("Rebuilding", inventory_filename)
        os.remove(inventory_filepath)

    if os.path.isfile(inventory_filepath):
        print('Using', inventory_filepath)
    else:
        print('Building', inventory_filename)
        target_database, target_duplicates = gather_inventory(target_path)
        if target_duplicates:
            delete_duplicates(target_duplicates)
        build_inventory(inventory_filepath, target_database)

    source_database, source_duplicates = gather_inventory(source_path)
    if source_duplicates:
        delete_duplicates(source_duplicates)

    # Don't perform move actions if the source and target are the same.
    if source_path != target_path:
        # Stash the data in the target, deduplicate and update inventory file.
        stash(source_database, target_path, inventory_filepath)

    # print("Done")
    input("Done")


if __name__ == '__main__':
    main()
