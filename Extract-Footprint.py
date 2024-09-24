import requests
import json
from time import sleep
import zipfile
import os
import pandas

USER_FOLDER = '/users/'+os.getenv('$USER')
TOKEN = os.getenv("DEMO_API_KEY", "blank")
#FOOTPRINT_ID= 'fps_2BE5QguvhDS9462rxAdy'  #Limited Footprint
FOOTPRINT_ID = "fps_2BE5arnbtGBwtp16D17H" #Full footprint

def fullurl(url):
    return 'https://api.watershedclimate.com/' +url

headers = {
  'accept': 'application/json',
  'content-type': 'application/json',
  'authorization': 'Bearer '+TOKEN,
}

def get(url):
    print("GET query to "+fullurl(url))
    request = requests.get(fullurl(url), headers=headers)
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return request.json()

def post(url, body):
    print("")
    print("POST query to "+fullurl(url))
    request = requests.post(fullurl(url), headers=headers, data=json.dumps(body))
    print(f'-> response: {request.status_code}')
    if request.status_code > 300:
        print(f'-> error: {request.text}')
    return request.json()

def unzip_file(zip_file_path, extract_to_directory):
    # Ensure the directory to extract to exists
    os.makedirs(extract_to_directory, exist_ok=True)

    # Open the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Extract all the contents into the specified directory
        zip_ref.extractall(extract_to_directory)
        print(f"Extracted all files to {extract_to_directory}")

def download_file(url, local_filename):
    # Send a GET request to the specified URL
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Raise an error for bad responses
        # Open a local file for writing in binary mode
        with open(local_filename, 'wb') as f:
            # Write the response content in chunks to the file
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {local_filename}")

def convert_csv_to_xlsx(csv_file_path, xlsx_file_path):
    # Read the CSV file into a DataFrame
    df = pandas.read_csv(csv_file_path, low_memory=False)

    # Write the DataFrame to an Excel file

    df.to_excel(xlsx_file_path, index=False, engine='openpyxl')

    print(f"File converted from {csv_file_path} to {xlsx_file_path}")

createFootprintExport = post(f'v2/reporting/export', {'footprintSnapshotId': FOOTPRINT_ID})
print("createFootprintExport", createFootprintExport)
downloadUrl = createFootprintExport["downloadUrl"]

print("Polling for export to be ready...")
while downloadUrl is None:
    downloadUrl = get(f'v2/reporting/export/{createFootprintExport["id"]}')["downloadUrl"]
    sleep(10)

print("Export is ready. Downloading...")

local_zip_filename = USER_FOLDER + '/Downloads/FootPrintDownloadDemo.zip'
download_file(downloadUrl, local_zip_filename)

working_directory = USER_FOLDER + '/downloads/extracted_demo_files'
unzip_file(local_zip_filename, working_directory)

#convert_csv_to_xlsx(working_directory+"/part-1.csv", working_directory+"/FootprintData.xlsx")