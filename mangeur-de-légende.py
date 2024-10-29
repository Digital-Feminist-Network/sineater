import fnmatch
import os
import re
import time

import gspread
from google.oauth2.service_account import Credentials


def authenticate_google_sheets(json_key_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(json_key_file, scopes=scope)
    client = gspread.authorize(credentials)
    return client


def get_post_id_from_filename(filename):
    match = re.search(
        r"uwaterlooconfessions-(\d+)-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}-UTC\.txt",
        filename,
    )
    return match.group(1) if match else None


def process_caption_file(file_path, worksheet):
    post_id = get_post_id_from_filename(os.path.basename(file_path))
    if not post_id:
        print(f"Skipping file with invalid filename format: {file_path}")
        return

    caption = open(file_path, "r", encoding="utf-8").read().strip()

    try:
        rows = worksheet.get_all_records()
        for row_idx, row in enumerate(
            rows, start=2
        ):  # Start at 2 to account for header row.
            if str(row["Post ID"]) == post_id and not row["Caption"]:
                worksheet.update_cell(row_idx, 4, caption)  # Caption is the 4th column.
                print(f"Added caption for Post ID {post_id}")
                return
        print(f"No empty caption slot found for Post ID {post_id} in sheet.")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")


def process_directory_for_captions(
    directory,
    json_key_file,
    sheet_name="uwaterlooconfessions",
    worksheet_name="confessions",
):
    client = authenticate_google_sheets(json_key_file)
    worksheet = client.open(sheet_name).worksheet(worksheet_name)

    for filename in os.listdir(directory):
        if fnmatch.fnmatch(filename, "uwaterlooconfessions-*UTC.txt"):
            file_path = os.path.join(directory, filename)
            process_caption_file(file_path, worksheet)
            time.sleep(1)  # Delay between files to reduce API rate limits.


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python process_captions.py <directory> <json_key_file>")
    else:
        directory = sys.argv[1]
        json_key_file = sys.argv[2]
        process_directory_for_captions(directory, json_key_file)
