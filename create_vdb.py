import os
import json
import sys
import os
import pickle
from langchain_community.document_loaders import JSONLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
import json
import boto3
from db_functions import *
from load_s3_file import load as lsf
import zipfile


def load_data(db, tb, file_name):
    # Connecting to Postgres and querying the table to get the Documentation data
    query = f"SELECT * FROM {tb}"
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

    print(f"\n-- {len(texts)} documents were loaded from the database successfully!")

    # Creating the json file
    json_output = {"title": titles, "text": texts, "emails": emails, "url": urls}
    with open(file_name, "w") as fout:
        json.dump(json_output, fout)

    print(f"\n-- The file {file_name} was created successfully!")


def create_vector_db(file_name, s3_bucket_name):

    print(f"\nLoading json file...")

    # Creating Docs
    loader = JSONLoader(file_path=f"./{file_name}", jq_schema=".text[]")

    data = loader.load()

    print(f"\n-- There are {len(data)} unnested docs.")

    # Initializing Embeddings model from locally cloned sentence_transformers repo
    embeddings = HuggingFaceEmbeddings(model_name="./all-mpnet-base-v2")

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=70)
    docs = text_splitter.split_documents(data)

    # Adding title to each doc's page_content
    file = open(f"{file_name}")
    confluence_json = json.load(file)
    file.close()
    for doc in docs:
        idx = doc.metadata["seq_num"] - 1
        cur_doc_title = str(confluence_json["title"][idx])
        doc.metadata["page_name"] = cur_doc_title

    print(f"\n-- The amount of docs inititally is: {len(docs)}.")

    # Concatenating small articles to large ones
    previous_sq_num = -1000
    concatenated_docs = []
    for i, doc in enumerate(docs):
        now_seq_num = doc.metadata["seq_num"]
        doc_name = doc.metadata["page_name"]
        if now_seq_num == previous_sq_num and len(doc.page_content) < 500:
            concatenated_docs.pop()
            docs[i - 1].page_content = (
                f"Topic: {doc_name} \n"
                + docs[i - 1].page_content
                + " "
                + docs[i].page_content
            )
            concatenated_docs.append(docs[i - 1])
        else:
            doc.page_content = f"\nTopic: {doc_name} \n" + doc.page_content
            concatenated_docs.append(doc)
        previous_sq_num = now_seq_num

    docs = concatenated_docs

    print(
        f"\n-- The amount of docs after concatenating the small articles is: {len(docs)}."
    )

    # Creating Vector Store to store docs embeddings
    db = FAISS.from_documents(docs, embeddings)
    # Creating a file to pickle the db object
    file = open("confluence_vdb", "wb")
    # dump information to that file
    pickle.dump(db, file)
    # close the file
    file.close()

    # Connecting to S3 Bucket and uploading the Vector DB
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    s3_client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url="https://s3url.com",
        verify=False,
    )
    s3_client.upload_file("./confluence_vdb", s3_bucket_name, "confluence_vdb")

    print(f"\n-- Vector DB created and stored to S3!")


if __name__ == "__main__":
    db = "ddl"
    file_name = "docs.json"
    table_schema = str(sys.argv[1])
    table_name = str(sys.argv[2])
    s3_bucket_name = str(sys.argv[3])
    tb = str(table_schema) + "." + str(table_name)
    model_file = "all-mpnet-base-v2.zip"

    print(f"file_name: {file_name}")
    print(f"table_schema: {table_schema}")
    print(f"table_name: {table_name}")
    print(f"s3_bucket_name: {s3_bucket_name}")
    print(f"Source table: {tb}")
    print(f"model_file: {model_file}")

    print("Loading Transformer model from S3...")
    lsf(model_file)
    with zipfile.ZipFile(model_file, "r") as zip_ref:
        zip_ref.extractall()

    print("Loading data from db...")
    load_data(db, tb, file_name)

    print("Creating Vector db and storing it to S3...")
    create_vector_db(file_name, s3_bucket_name)
