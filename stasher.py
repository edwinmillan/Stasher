import os
import hashlib
import argparse
from tinydb import TinyDB, where
from pprint import pprint
from shutil import move


def get_args():
    parser = argparse.ArgumentParser(description='Duplicate checker')
    parser.add_argument('path', nargs='?', default=os.getcwd(), help='Path to files, defaults to current directory')
    args = parser.parse_args()
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
                print(f'\tMarking for Deletion: {data["full_path"]}')
                duplicates.append(data)
            else:
                # Store the collected data in the database
                database.append(data)
    return database, duplicates


def main():
    # args = get_args()
    # path = args.path.replace('\\', '\\\\')
    
    database, duplicates = gather_inventory(r'E:\Projects\Stasher\testfiles')

    # pprint(database)
    # pprint(duplicates)


if __name__ == '__main__':
    main()
