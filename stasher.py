import os
import hashlib
import argparse
from tinydb import TinyDB, where
from pprint import pprint
from shutil import move


def get_args():
    parser = argparse.ArgumentParser(description='Duplicate checker')
    parser.add_argument('-s', '--source', default=os.getcwd(), help='Path to source dir, defaults to current directory')
    parser.add_argument('-t', '--target', help='Path to target dir, defaults to source directory')
    parser.add_argument('-b', '--build-inventory', action='store_true', help='Builds inventory file')

    args = parser.parse_args()
    # Set target to the value of source if it's not provided.
    args.target = args.target if args.target else args.source

    pprint(args)
    return args


def chunk_reader(file, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        datachunk = file.read(chunk_size)
        if not datachunk:
            return
        yield datachunk


def gather_inventory(source_path):
    database = []
    duplicates = []
    for dirpath, dirnames, filenames in os.walk(source_path):
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
                print(f'\tMarking for Deletion: {data["full_path"]}')
                duplicates.append(data)
            else:
                # Store the collected data in the database
                database.append(data)
    return database, duplicates


def delete_duplicates(duplicates_list):
    for duplicate in duplicates_list:
        print(f'Deleting {duplicate["filename"]}')
        os.remove(duplicate['full_path'])


def main():
    args = get_args()
    source_path = args.source.replace('\\', '\\\\')
    target_path = args.target.replace('\\', '\\\\')

    database, duplicates = gather_inventory(source_path)

    if duplicates:
        delete_duplicates(duplicates)

    # if args.build_inventory:
    #     pass

    # pprint(database)
    # pprint(duplicates)


if __name__ == '__main__':
    main()
