import fnmatch
import json
import os
import re
import time
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials


# Authenticate and access Google Sheets.
def authenticate_google_sheets(json_key_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(json_key_file, scopes=scope)
    client = gspread.authorize(credentials)
    return client


def get_existing_comment_ids(sheet):
    """Fetch all existing Comment IDs in the Google Sheet to avoid duplicates."""
    print("Fetching existing Comment IDs...")
    all_ids = sheet.col_values(3)  # Assumes "Comment ID" is the 3rd column.
    return set(all_ids)


# Try not to blow up Google Sheets API.
def retry_append_with_backoff(sheet, rows, retries=3):
    """Attempt to append data with exponential backoff on API errors."""
    for i in range(retries):
        try:
            sheet.append_rows(rows, value_input_option="USER_ENTERED")
            print(f"Appended {len(rows)} rows successfully.")
            return True
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                wait = 2**i  # Exponential backoff.
                print(f"Quota exceeded. Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                raise e
    print("Failed to append due to quota limits.")
    return False


def batch_append_to_sheet(sheet, rows):
    """Batch append rows to reduce API calls."""
    if rows:
        retry_append_with_backoff(sheet, rows)


def convert_to_utc(timestamp):
    """Convert timestamp to UTC format."""
    return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def extract_post_id(filename):
    """Extract post_id from filename using regex."""
    match = re.search(r"uwaterlooconfessions-(\d+)-", filename)
    if match:
        return match.group(1)
    return None


def process_comment_file(file_path, sheet, existing_comment_ids):
    """Process each JSON file, batch comments, and prepare for Google Sheet."""
    with open(file_path, "r") as f:
        comments = json.load(f)

    # Extract Post ID from filename and format as text.
    post_id = extract_post_id(os.path.basename(file_path))
    if not post_id:
        print(f"Could not extract post ID from {file_path}")
        return

    new_entries = []
    for comment in comments:
        comment_id = comment["id"]
        # Skip if Comment ID already exists in Google Sheet.
        if comment_id in existing_comment_ids:
            continue

        # Extract required fields and format IDs as text.
        post_timestamp = convert_to_utc(comment["created_at"])
        commenter = comment["owner"]["username"]
        like_count = comment["likes_count"]
        text = comment["text"]

        # Add data to batch for appending.
        # Be nice to Google Sheets API.
        new_entries.append(
            [
                post_timestamp,
                f"'{post_id}",  # Format Post ID as text.
                f"'{comment_id}",  # Format Comment ID as text.
                commenter,
                like_count,
                text,
            ]
        )

        # Add Comment ID to local set to prevent duplicates in a given run.
        existing_comment_ids.add(comment_id)

    # Batch append new entries to Google Sheets.
    batch_append_to_sheet(sheet, new_entries)


def process_directory(directory, json_key_file):
    """Process all JSON files in directory and append unique comments to Google Sheet."""
    client = authenticate_google_sheets(json_key_file)
    sheet = client.open("uwaterlooconfessions").worksheet("comments")

    # Get existing Comment IDs to minimize read calls.
    existing_comment_ids = get_existing_comment_ids(sheet)

    # Loop through all comment JSON files in directory.
    for filename in os.listdir(directory):
        if fnmatch.fnmatch(filename, "uwaterlooconfessions-*comments.json"):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {filename}")
            process_comment_file(file_path, sheet, existing_comment_ids)
            time.sleep(1)  # Delay between files to reduce API rate limits.


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python script.py <directory> <google_api_json_key_file>")
        sys.exit(1)

    directory = sys.argv[1]
    json_key_file = sys.argv[2]
    process_directory(directory, json_key_file)
