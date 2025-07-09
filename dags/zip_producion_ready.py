#!/usr/bin/env python3

import sys
import zipfile
import os
import json
import xml.etree.ElementTree as ET
from io import BytesIO
import tempfile
import shutil

# In S3FileTransformOperator:
# - First argument (sys.argv[1]) is the source file path
# - Second argument (sys.argv[2]) is the destination file path

# def xml_to_json(xml_content):
#     """Convert XML content to JSON format"""
#     try:
#         root = ET.fromstring(xml_content)
#
#         # Function to convert element to dict
#         def element_to_dict(element):
#             result = {}
#
#             # Add attributes
#             for key, value in element.attrib.items():
#                 result[f"@{key}"] = value
#
#             # Add children
#             for child in element:
#                 child_dict = element_to_dict(child)
#                 if child.tag in result:
#                     if not isinstance(result[child.tag], list):
#                         result[child.tag] = [result[child.tag]]
#                     result[child.tag].append(child_dict)
#                 else:
#                     result[child.tag] = child_dict
#
#             # Add text content if no children
#             if len(result) == 0 and element.text and element.text.strip():
#                 return element.text.strip()
#             elif len(result) == 0:
#                 return None
#
#             return result
#
#         return {root.tag: element_to_dict(root)}
#     except Exception as e:
#         print(f"Error converting XML to JSON: {e}")
#         return {"error": str(e)}

import xmltodict
# dev dependency for checking xmltodict output
# need to install xmltodict into Docker image before uncommenting
def xml_to_json(xml_content):
    return xmltodict.parse(xml_content)

def process_files(source_path, destination_path, *script_args):
    print("="*100)
    print("Processing ZIP file from S3")
    print(f"Source file path: {source_path}")
    print(f"Destination file path: {destination_path}")
    print("="*100)

    # Create a temporary directory to extract files
    temp_dir = tempfile.mkdtemp()

    try:
        # Read the source ZIP file
        with open(source_path, 'rb') as f:
            source_data = f.read()

        # Extract files from source ZIP
        with zipfile.ZipFile(BytesIO(source_data), 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Process each file in the extracted directory
        for filename in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, filename)

            # If it's an XML file, convert it to JSON
            if filename.endswith('.xml'):
                with open(file_path, 'r', encoding='utf-8') as xml_file:
                    xml_content = xml_file.read()

                # Convert XML to JSON
                json_content = xml_to_json(xml_content)

                # Create a JSON file
                json_filename = os.path.splitext(filename)[0] + '.json'
                json_path = os.path.join(temp_dir, json_filename)

                with open(json_path, 'w', encoding='utf-8') as json_file:
                    json.dump(json_content, json_file, indent=2)

        # Create a new ZIP file with all files (including the new JSON files)
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as new_zip:
            for filename in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, filename)
                if os.path.isfile(file_path):
                    new_zip.write(file_path, arcname=filename)

        # Write the new ZIP to the destination
        with open(destination_path, 'wb') as f:
            f.write(zip_buffer.getvalue())

        print(f"Successfully processed ZIP file and created new ZIP with JSON conversions")

    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Error: Missing required arguments")
        print("Usage: python unzip_convert_xml_to_json_rezip.py <source_file> <destination_file> [<script_args>...]")
        print("Have been given:", sys.argv)
        sys.exit(1)

    source_path, destination_path, *script_args = sys.argv[1:]
    process_files(source_path, destination_path, *script_args)