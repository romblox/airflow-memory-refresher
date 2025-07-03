#!/usr/bin/env python3

import sys
import zipfile
from io import BytesIO

# In S3FileTransformOperator:
# - First argument (sys.argv[1]) is the source file path
# - Second argument (sys.argv[2]) is the destination file path

def zip_file(source_path, destination_path):
    # Create a BytesIO object to store the zip file in memory
    zip_buffer = BytesIO()

    # Create a ZipFile object
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
        # Get the filename from the source path
        filename = source_path.split('/')[-1]

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
    if len(sys.argv) != 3:
        print("Usage: python zip_transform.py <source_file> <destination_file>")
        sys.exit(1)

    source_path = sys.argv[1]
    destination_path = sys.argv[2]

    zip_file(source_path, destination_path)
