import time

import click
import gspread
from alive_progress import alive_bar
from google.oauth2.service_account import Credentials
from transformers import pipeline


def authenticate_google_sheets(json_key_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(json_key_file, scopes=scope)
    client = gspread.authorize(credentials)
    return client


@click.command()
@click.option("--sheet-id", required=True, help="Google Sheet ID.")
@click.option("--gid", required=True, type=int, help="Worksheet GID.")
@click.option("--column", required=True, help="Name of the column to classify.")
@click.option(
    "--json-key-file", required=True, help="Path to Google service account JSON key."
)
@click.option(
    "--batch-size", default=30, help="Number of rows to batch before writing."
)
def main(sheet_id, gid, column, json_key_file, batch_size):
    client = authenticate_google_sheets(json_key_file)
    sheet = client.open_by_key(sheet_id)
    worksheet = next((ws for ws in sheet.worksheets() if ws.id == gid), None)

    if worksheet is None:
        click.echo(f"Worksheet with gid {gid} not found.")
        return

    records = worksheet.get_all_records()
    if not records:
        click.echo("Sheet is empty.")
        return

    headers = list(records[0].keys())

    if "Hate Label" not in headers:
        worksheet.update_cell(1, len(headers) + 1, "Hate Label")
        headers.append("Hate Label")
    if "Hate Score" not in headers:
        worksheet.update_cell(1, len(headers) + 1, "Hate Score")
        headers.append("Hate Score")

    label_col_index = headers.index("Hate Label") + 1
    score_col_index = headers.index("Hate Score") + 1
    comment_col_index = headers.index(column)

    model_pipeline = pipeline(
        "text-classification", model="facebook/roberta-hate-speech-dynabench-r4-target"
    )

    updates = []
    with alive_bar(len(records), title="Classifying and Updating") as bar:
        for i, row in enumerate(records):
            row_num = i + 2
            comment = str(row.get(column, "")).strip()

            if not comment:
                bar()
                continue

            if row.get("Hate Label") and row.get("Hate Score") is not None:
                bar()
                continue

            result = model_pipeline(comment, truncation=True)
            label = "nothate"
            hate_score = 0.0

            for entry in result:
                if entry["label"] == "hate":
                    hate_score = entry["score"]
                    label = "hate"
                elif entry["label"] == "nothate":
                    hate_score = entry["score"]
                    label = "nothate"

            updates.append(
                {
                    "range": gspread.utils.rowcol_to_a1(row_num, label_col_index),
                    "values": [[label]],
                }
            )
            updates.append(
                {
                    "range": gspread.utils.rowcol_to_a1(row_num, score_col_index),
                    "values": [[hate_score]],
                }
            )

            if len(updates) >= batch_size * 2:
                worksheet.batch_update(updates, value_input_option="RAW")
                updates = []
                time.sleep(2.5)

            bar()

        if updates:
            worksheet.batch_update(updates, value_input_option="RAW")


if __name__ == "__main__":
    main()
