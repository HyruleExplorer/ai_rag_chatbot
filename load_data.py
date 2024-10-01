import os
import json
from db_functions import *

db = "pg"
table = os.environ.get("SOURCE_TB")
query = f"SELECT * FROM {table}"


def load(log_file):
    # Connecting to Postgres and querying the table to get the Documentation data
    with open(f"./{log_file}", "a") as f:
        f.write(f"\nRunning query on database...")
    df = get_query(db, query)

    # Turning the df columns into lists
    titles = df["title"].tolist()
    texts = df["text"].tolist()
    emails = df["emails"].tolist()
    urls = df["url"].tolist()
    sources = df["source"].tolist()

    # Removing docs shorter than 300 characters
    for idx, text in enumerate(texts):
        if len(text) < 300:
            titles.pop(idx)
            texts.pop(idx)
            emails.pop(idx)
            urls.pop(idx)
            sources.pop(idx)

    with open(f"./{log_file}", "a") as f:
        f.write(f"\n{len(texts)} documents were loaded from the database successfully!")

    # Creating the json file
    json_output = {"title": titles, "text": texts, "emails": emails, "url": urls}
    with open(f"./docs.json", "w") as fout:
        json.dump(json_output, fout)

    with open(f"./{log_file}", "a") as f:
        f.write("\nThe file docs.json was created successfully!")


if __name__ == "__main__":
    log_file = "init.log"
    with open(f"./{log_file}", "w") as f:
        f.write("\nStarting load_data.py...")

    load()

    with open(f"./{log_file}", "a") as f:
        f.write("\nLoad_data completed!")
