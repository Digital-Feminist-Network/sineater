import os
import re
from collections import Counter
from datetime import datetime

import click
import gspread
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from alive_progress import alive_bar
from google.oauth2.service_account import Credentials
from wordcloud import WordCloud


def slugify(text):
    return re.sub(r"[^a-zA-Z0-9\-]+", "-", text.strip().lower()).strip("-")


def authenticate_google_sheets(json_key_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(json_key_file, scopes=scope)
    client = gspread.authorize(credentials)
    return client


def fetch_sheet_data(sheet_id, gid, json_key_file):
    client = authenticate_google_sheets(json_key_file)
    sheet = client.open_by_key(sheet_id)
    worksheet = next(
        (ws for ws in sheet.worksheets() if ws._properties["sheetId"] == int(gid)), None
    )
    if not worksheet:
        raise ValueError(f"Worksheet with gid {gid} not found.")
    return worksheet.get_all_records()


def generate_pie_chart(data, column, title, output_path):
    counts = Counter(row[column] for row in data if row[column])
    labels, values = zip(*counts.items())

    custom_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3"]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.3,
            textinfo="percent+label",
            marker=dict(colors=custom_colors),
            textfont=dict(size=20, family="Arial"),
        )
    )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    footer_text = f"Generated: {timestamp}"

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            xanchor="center",
            yanchor="top",
            font=dict(size=28, family="Arial"),
        ),
        annotations=[
            dict(
                x=1,
                y=-0.05,
                xref="paper",
                yref="paper",
                text=footer_text,
                showarrow=False,
                font=dict(size=12, color="gray"),
                align="right",
            )
        ],
        height=1300,
        width=1500,
        margin=dict(l=50, r=50, t=200, b=100),
        showlegend=False,
    )

    fig.write_html(output_path)


def generate_wordcloud(
    text, output_path, file_count, title, width=2560, height=1440, stop_words=None
):
    wordcloud = WordCloud(
        width=width, height=height, background_color="white", stopwords=stop_words
    ).generate(text)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metadata_text = f"Generated: {timestamp}\nTranscripts: {file_count}"

    plt.figure(figsize=(width / 100, height / 100 + 1), dpi=100)
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")

    plt.title(
        "\n" + title + "\n",
        fontsize=36,
        color="darkblue",
        fontweight="bold",
    )

    plt.figtext(
        0.90,
        0.10,
        metadata_text,
        horizontalalignment="right",
        fontsize=10,
        color="gray",
        wrap=True,
    )

    plt.savefig(output_path, bbox_inches="tight")
    plt.close()


@click.command()
@click.option("--sheet-id", required=True, help="Google Sheet ID.")
@click.option("--gid", required=True, type=int, help="Worksheet GID.")
@click.option("--title", required=True, help="Chart title.")
@click.option(
    "--chart",
    required=True,
    type=click.Choice(
        ["commenters", "comments-wordcloud", "confession-wordcloud",
         "misogyny", "hate", "sexism", "homophobia"]
    ),
    help="Chart type.",
)
@click.option(
    "--json-key-file",
    required=True,
    help="Path to your Google service account key file.",
)
def generate_chart(sheet_id, gid, title, chart, json_key_file):
    with alive_bar(1, title="Fetching Google Sheet data...") as bar:
        data = fetch_sheet_data(sheet_id, gid, json_key_file)
        bar()

    output_slug = slugify(title)
    output_name = (
        f"{output_slug}.png" if "wordcloud" in chart else f"{output_slug}.html"
    )

    with alive_bar(1, title="Generating chart...") as bar:
        if chart == "commenters":
            generate_pie_chart(data, "Commenter", title, output_name)
        elif chart == "misogyny":
            generate_pie_chart(data, "Misogyny Label", title, output_name)
        elif chart == "hate":
            generate_pie_chart(data, "Hate Label", title, output_name)
        elif chart == "sexism":
            generate_pie_chart(data, "Sexism Label", title, output_name)
        elif chart == "homophobia":
            generate_pie_chart(data, "Homophobia Label", title, output_name)
        elif chart == "comments-wordcloud":
            text = " ".join(str(row["Comment"]) for row in data if row["Comment"])
            generate_wordcloud(text, output_name, len(data), title)
        elif chart == "confession-wordcloud":
            text = " ".join(
                row["OCR'd Confession Text"]
                for row in data
                if row["OCR'd Confession Text"]
            )
            generate_wordcloud(text, output_name, len(data), title)
        bar()

    click.echo(f"Visualization saved as: {output_name}")


if __name__ == "__main__":
    generate_chart()
