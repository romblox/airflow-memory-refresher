#!/usr/bin/env python3

import sys
import zipfile
from importlib.util import source_hash
from io import BytesIO

# In S3FileTransformOperator:
# - First argument (sys.argv[1]) is the source file path
# - Second argument (sys.argv[2]) is the destination file path

def zip_file(source_path, destination_path, *script_args):
    # Create a BytesIO object to store the zip file in memory
    zip_buffer = BytesIO()


    print("="*100)
    print("Catch arguments:", script_args)
    print(f"Source file path: {source_path} with type of {type(source_path)}")
    print(f"Destination file path: {destination_path} with type of {type(source_path)}")
    print(f"Creating zip file from {source_path} and writing it to {destination_path}")
    print("="*100)


    # Create a ZipFile object
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
        # Get the filename from the source path
        # filename = source_path.split('/')[-1]
        filename = script_args[0]

        # Read the source file
        with open(source_path, 'rb') as f:
            file_data = f.read()

        # Add the file to the zip archive
        zip_file.writestr(filename, file_data)

    # Move the buffer position to the beginning
    zip_buffer.seek(0)

    # Write the zip buffer to the destination file
    with open(destination_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Missing required arguments")
        print("Usage: python zip_transform.py <source_file> <destination_file> <script_args>")
        print("Have been given:", sys.argv)
        sys.exit(1)

    # print("+"*100)
    # print(sys.argv)
    # print("+"*100)


    # source_path = sys.argv[1]
    # destination_path = sys.argv[2]
    # # Capture any additional arguments
    # script_args = sys.argv[3:] if len(sys.argv) > 3 else []

    source_path, destination_path, *script_args = sys.argv[1:]

    zip_file(source_path, destination_path, *script_args)
