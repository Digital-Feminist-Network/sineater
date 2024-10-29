import os
import sys

import gspread
import pytesseract
from google.oauth2.service_account import Credentials

pytesseract.pytesseract.tesseract_cmd = r"/usr/bin/tesseract"


# Authenticate and access Google Sheets.
def authenticate_google_sheets(json_key_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(json_key_file, scopes=scope)
    client = gspread.authorize(credentials)
    return client


def process_filename(filename):
    """Extract post_id and post_date from the filename."""
    parts = filename.split("-")
    post_id = parts[1]  # Get post_id part.
    post_date = "-".join(parts[2:]).replace(".jpg", "")  # Get the entire date.
    return post_id, post_date


def ocr_image(image_path):
    """Use Tesseract to extract text from the image with specified flags."""
    try:
        custom_flags = "--oem 3 --psm 6"
        text = pytesseract.image_to_string(image_path, config=custom_flags)
        return text.strip()
    except Exception as e:
        print(f"Error processing image {image_path}: {e}")
        return ""


def check_and_append_rows(directory_path, sheet):
    """Process images and append missing rows to Google Sheet."""
    # Fetch the list of filenames in the first column ('Filename').
    existing_filenames = sheet.col_values(
        1
    )  # Fetch all filenames from the 'Filename' column.

    for image_file in os.listdir(directory_path):
        if image_file.endswith(".jpg"):
            if image_file not in existing_filenames:
                # Extract post_id and post_date.
                post_id, post_date = process_filename(image_file)

                # Do OCR on the image.
                image_path = os.path.join(directory_path, image_file)
                confession_text = ocr_image(image_path)

                # Append row.
                # The order should match: 'Filename', 'Post ID', 'Post date', 'OCR'd Confession Text'.
                sheet.append_row([image_file, post_id, post_date, confession_text])
                print(f"Added: {image_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python confess-ocr.py <directory_path> <json_key_file>")
        sys.exit(1)

    directory_path = sys.argv[1]
    json_key_file = sys.argv[2]

    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory.")
        sys.exit(1)

    client = authenticate_google_sheets(json_key_file)
    sheet = client.open("uwaterlooconfessions").worksheet("confessions")

    check_and_append_rows(directory_path, sheet)

    print("Processing completed.")
