import pandas as pd
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm  # For progress bar
from tabulate import tabulate  # For table formatting in the terminal
import socket
import sys
import time  # For adding a delay (backoff) between retries


# Define file paths
source_file = 'GRI_2017_2020.xlsx'
destination_file = 'GRI_2017_2020_fixed.xlsx'
status_file = 'download_status.xlsx'
pdf_download_folder = 'PDF Downloads'
os.makedirs(pdf_download_folder, exist_ok=True)  # Ensure the download folder exists

# Function to check if there is an active internet connection
def has_internet_connection():
    try:
        # Attempt to connect to a known public DNS server (Google's)
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        return True
    except OSError:
        return False


# Check if the destination file already exists
if not os.path.exists(destination_file):
    try:
        # Read and fix the Excel file as before
        df = pd.read_excel(source_file)
        df.set_index("BRnum", inplace=True)  # Ensure indexing by BRnum
        total_rows = len(df)

        for index, row in df.iterrows():
            progress = (index + 1) / total_rows * 100
            print(f"\rProcessing row {index + 1}/{total_rows} ({progress:.2f}%)", end='')

            type_of_assurance_value = row['Type of Assurance Provider']
            external_assurance_value = row['External Assurance']

            if type_of_assurance_value in ['Yes', 'No']:
                if external_assurance_value in ['Yes', 'No']:
                    continue
                elif pd.isna(external_assurance_value):
                    df.loc[index, 'External Assurance'] = type_of_assurance_value
                    df.loc[index, 'Type of Assurance Provider':] = row['Type of Assurance Provider':].shift(-1)

        df.to_excel(destination_file, index=True)
        print("\nThe Excel file has been fixed and saved as", destination_file)

    except FileNotFoundError:
        print(f"Error: The file {source_file} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")


# Initialize or load the download status file
if os.path.exists(status_file):
    print(f"\nLoading {status_file}...\n")
    download_status_df = pd.read_excel(status_file, engine='openpyxl', index_col="BRnum")
    if 'File Size' not in download_status_df.columns:
        download_status_df['File Size'] = "N/A"
else:
    print(f"\nLoading {destination_file}...\n")
    df_fixed = pd.read_excel(destination_file, engine='openpyxl', index_col="BRnum")
    download_status_df = pd.DataFrame({
        'BRnum': df_fixed.index,
        'Download Status': 'Ikke downloadet'
    })
    download_status_df.set_index("BRnum", inplace=True)  # Ensure indexing by BRnum

# Filter rows where status is "Ikke downloadet" and Pdf_URL is not blank
print(f"\nLoading {destination_file}...\n")
df_fixed = pd.read_excel(destination_file, engine='openpyxl', index_col="BRnum")
downloadable_rows = df_fixed[(df_fixed['Pdf_URL'].notna()) &
                             (download_status_df['Download Status'] == 'Ikke downloadet')]

# Display the number of remaining downloadable rows
remaining_downloads = len(downloadable_rows)
print(f"Total PDFs remaining to download: {remaining_downloads}")

# Set the number of PDFs to download with a default of 20
try:
    num_pdfs_to_download = int(input(f"Enter the number of PDFs to download (default is 20): ") or 20)
except ValueError:
    num_pdfs_to_download = 20  # If the input is not a valid integer, default to 20

print(f"Number of PDFs to download this session: {num_pdfs_to_download}")

# Limit the number of rows to download based on num_pdfs_to_download
selected_rows = downloadable_rows.head(num_pdfs_to_download)

# Modified download function with internet connection check
def download_pdf(row):
    brnum = row.name
    pdf_url = row['Pdf_URL']
    alt_url = row.get('Report Html Address')
    pdf_filename = f"{pdf_download_folder}/{brnum}.pdf"
    status_message = 'Downloadet'
    max_attempts = 3
    timeout = 5
    file_size_kb = "N/A"

    def attempt_download(url):
        nonlocal status_message, file_size_kb
        for attempt in range(1, max_attempts + 1):
            if not has_internet_connection():
                start_time = time.time()
                countdown_seconds = 180  # 3 minutes in seconds

                # Wait until internet is back or until 3 minutes pass
                while not has_internet_connection():
                    elapsed = time.time() - start_time
                    remaining = countdown_seconds - int(elapsed)
                    if remaining <= 0:
                        print("\nNo internet connection. Program timed out.")
                        sys.exit()  # Exit the program after 3 minutes without internet

                    print(f"\rNo internet connection. Retrying in {remaining} seconds...", end="")
                    time.sleep(1)  # Update the countdown every second

                print("\nInternet connection restored. Resuming downloads...")

            try:
                with requests.get(url, timeout=timeout, stream=True) as response:
                    response.raise_for_status()

                    if response.headers.get('Content-Type') != 'application/pdf':
                        status_message = 'Invalid Download (Not a PDF file)'
                        break

                    with open(pdf_filename, 'wb') as pdf_file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                pdf_file.write(chunk)

                    file_size_kb = os.path.getsize(pdf_filename) / 1024
                    return True

            except requests.exceptions.Timeout:
                status_message = 'Invalid Download (Timeout)'
            except requests.exceptions.HTTPError as http_err:
                status_message = f'Invalid Download (HTTP Error {response.status_code})'
            except requests.exceptions.ConnectionError:
                status_message = 'Invalid Download (Connection Error)'
            except requests.exceptions.RequestException as req_err:
                status_message = f'Invalid Download (Other Error: {req_err})'
            
            time.sleep(1)

        return False

    if pd.notna(pdf_url) and pdf_url and attempt_download(pdf_url):
        return brnum, status_message, f"{file_size_kb:.2f} KB"

    if pd.notna(alt_url) and alt_url and attempt_download(alt_url):
        return brnum, status_message, f"{file_size_kb:.2f} KB"

    return brnum, status_message, file_size_kb



# Use ThreadPoolExecutor for multithreading
with ThreadPoolExecutor(max_workers=10) as executor:
    # Progress bar for tracking completed tasks
    with tqdm(total=len(selected_rows), desc="Download Progress", unit="file") as pbar:
        # Submit each row in selected_rows for downloading
        futures = {executor.submit(download_pdf, row): row.name for _, row in selected_rows.iterrows()}
        
        for future in as_completed(futures):
            brnum, result_status, file_size = future.result()
            download_status_df.loc[brnum, 'Download Status'] = result_status
            download_status_df.loc[brnum, 'File Size'] = file_size  # Add file size to DataFrame
            pbar.update(1)



# Save the updated download statuses back to download_status.xlsx
download_status_df.to_excel(status_file, index=True, engine='openpyxl')
print("\nDownload status updated in", status_file)


# Display the final status of only the selected rows in table format
print("\nFinal Download Status for Selected Files:\n")
selected_status_df = download_status_df.loc[selected_rows.index][['Download Status', 'File Size']]  # Filter by selected_rows index and include File Size
print(tabulate(selected_status_df.reset_index(), headers="keys", tablefmt="grid"))
