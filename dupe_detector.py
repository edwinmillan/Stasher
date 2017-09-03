import os
import hashlib
import argparse


def get_args():
    parser = argparse.ArgumentParser(description='Duplicate checker')
    parser.add_argument('path', default=os.getcwd(), help='Path to files, defaults to current directory')
    args = parser.parse_args()
    return args


def chunk_reader(file, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        datachunk = file.read(chunk_size)
        if not datachunk:
            return
        yield datachunk


def check_for_duplicates(path):
    hashes = {}
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            # Create an empty hashlib hash for filling
            sha_hash = hashlib.sha512()
            # Create the full filepath
            full_path = os.path.join(dirpath, filename)
            # Load up the files into the chunk reader
            with open(full_path, 'rb') as file:
                for datachunk in chunk_reader(file):
                    sha_hash.update(datachunk)
            # digest that juicy file,
            file_hash = sha_hash.digest()
            duplicate_file_path = hashes.get(file_hash)  # Use get, so a KeyError isn't received if it doesn't exist
            if duplicate_file_path:
                print(f'Duplicate found: {full_path} and {duplicate_file_path}')
            else:
                hashes[file_hash] = full_path


def main():
    args = get_args()
    path = args.path.replace('\\', '\\\\')
    check_for_duplicates(path)

if __name__ == '__main__':
    main()
