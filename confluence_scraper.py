import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import re
import sys
import json
import time
from db_functions import *


def html_to_text(table: str):
    if "</th>" in table:
        output_df = pd.read_html(table)[0]
    else:
        output_df = pd.read_html(table, skiprows=1)[0]

    output_list = []

    output_df = output_df.to_json(orient="records")

    for el in json.loads(output_df):
        output_list.append(str(el)[1:-1].replace("'", ""))

    return ";\n".join(output_list)


def backoff(response, calls, header, url, time_to_wait=3):
    # if page_count > 95, wait for 1 minute:
    new_call_count = calls
    new_response = response
    rate_limit = 96

    # Sleeping for one second regardless of the case
    # time.sleep(time_to_wait)

    # Possible cases:
    # Return code 200, call count > rate_limit -> Wait one minute and reset call count
    # Return code 200, call count <= rate_limit -> Increase call count and continue
    # Return code not 200, call count > rate_limit -> Wait one minute, retry until 200 and reset call count
    # Return code not 200, call count <= rate_limit -> Increase call count, Wait few seconds and retry until 200
    if response.status_code == 200 and calls > rate_limit:
        print(
            f"API Call successfull! {rate_limit} calls per minute reached. Waiting for 1 minute. Current API call count: {calls}. URL: {url}."
        )
        time.sleep(60)
        new_call_count = 1

    elif response.status_code == 200 and calls <= rate_limit:
        # print( f'API Call successfull! Current API call count: {calls}')
        new_call_count = calls + 1

    elif response.status_code != 200 and calls > rate_limit:
        print(
            f"API Call error! Code: {response.status_code}. {rate_limit} calls per minute reached. Waiting for 1 minute and retrying. Current API call count: {calls}. URL: {url}."
        )
        time.sleep(60)
        new_response = requests.get(url, headers=header, verify=False)
        while new_response.status_code != 200:
            print(
                f"API Call error! Code: {response.status_code}. Waiting {time_to_wait} seconds and retrying. Current API call count: {calls}. URL: {url}."
            )
            time.sleep(time_to_wait)
            time_to_wait = time_to_wait * 2
            new_response = requests.get(url, headers=header, verify=False)
        new_call_count = 1

    elif response.status_code != 200 and calls <= rate_limit:
        while response.status_code != 200:
            print(
                f"API Call error! Code: {response.status_code}. Waiting {time_to_wait} seconds and retrying. Current API call count: {calls}. URL: {url}."
            )
            time.sleep(time_to_wait)
            time_to_wait = time_to_wait * 2
            new_response = requests.get(url, headers=header, verify=False)
        new_call_count = calls + 1

    else:
        print(
            f"Unhandled case in backoff function. Return Code: {response.status_code}. Current API call count: {calls}. URL: {url}."
        )

    # Print the content of the response (the data the server returned)
    # print("API Call response content: "+str(new_response.json()))
    return new_call_count, new_response


def nextedjson_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


def GetPageContent(SPACEKEY, header):
    requests.packages.urllib3.disable_warnings(
        requests.packages.urllib3.exceptions.InsecureRequestWarning
    )
    # API to get page content
    APIpageContent = "https://confluence.com/rest/api/content/{}?expand=body.storage"
    APISPACE = "https://confluence.com/rest/api/space/{}/content/page?limit={}&start={}"
    limitsize = 20
    api_calls = 0
    StartPosition = 0
    SpaceLoop = True
    TotalPageCount = 0

    titles, texts, emails, urls = [], [], [], []

    while SpaceLoop:
        try:
            APISPACE = "https://confluence.com/rest/api/space/{}/content/page?limit={}&start={}".format(
                SPACEKEY, limitsize, StartPosition
            )
            JSONResult = requests.get(APISPACE, headers=header, verify=False)

            # Exponential backoff to avoid API limit (error 429)
            api_calls, JSONResult = backoff(JSONResult, api_calls, header, APISPACE)

            # loop drop page read
            ContentIDList = nextedjson_extract(JSONResult.json(), "id")

            for ContentID in ContentIDList:
                try:

                    print("Scraping page: " + str(ContentID))
                    APIpageContent = "https://confluence.com/rest/api/content/{}?expand=body.storage".format(
                        ContentID
                    )
                    PageJSONResult = requests.get(
                        APIpageContent, headers=header, verify=False
                    )

                    # Exponential backoff to avoid API limit (error 429)
                    api_calls, PageJSONResult = backoff(
                        PageJSONResult, api_calls, header, APIpageContent
                    )

                    PageUrl = ""
                    PageTitle = ""
                    PageContent = ""
                    try:
                        PageUrl = PageJSONResult.json()["_links"]
                        PageUrl = PageUrl["base"] + PageUrl["webui"]

                        PageTitle = nextedjson_extract(PageJSONResult.json(), "title")[
                            0
                        ]
                        PageContent = nextedjson_extract(
                            PageJSONResult.json(), "value"
                        )[0]
                    except Exception as e:
                        print("Error while reading page content: ")
                        print(e)
                        break

                    table_regex = r"<table(.*?)table>"
                    strikeout_regex = r"<s\b[^>]*>(.*?)<\/s>"

                    # processing tables
                    tables_str, tables_error = [], []
                    tables = re.findall(table_regex, PageContent)

                    page_processed = PageContent

                    if tables != []:
                        for table in tables:
                            try:
                                tables_str.append(
                                    html_to_text("<table" + table + "table>")
                                )
                            except Exception as e:
                                tables_error.append("<table" + table + "table>")

                    if tables_str != []:
                        for table, table_str in zip(tables, tables_str):
                            page_processed = str(page_processed).replace(
                                "<table" + table + "table>", table_str
                            )

                    if tables_error != []:
                        for table in tables_error:
                            page_processed = str(page_processed).replace(
                                "<table" + table + "table>", ""
                            )

                    # Removing strikeout lines
                    page_processed = re.sub(strikeout_regex, "", page_processed)

                    # Removing HTML tags
                    for s in ["<strong>", "</strong>", "<![CDATA[", "]]>", "Midnight"]:
                        page_processed = page_processed.replace(s, "")

                    # Replacing HTML line breaks with text newline character
                    delimiter = "\n"
                    for s in ["<br />", "<br/>", "<br>"]:
                        page_processed = page_processed.replace(s, delimiter)

                    soup = BeautifulSoup(page_processed, "lxml")
                    text = soup.get_text(separator=" ").replace("\xa0", " ")

                    title = PageTitle
                    for s in ["<>", ":", "&", "//", "\xa0", "?", "/", ">>", "->"]:
                        title = title.replace(s, "-")

                    title = title.strip()

                    links = []

                    for link in soup.find_all("a", href=True):
                        links.append(link["href"])

                    if len(links):
                        links = pd.Series(links)
                        links = links[links.str.startswith("mail")].apply(
                            lambda x: x.split(":")[1]
                        )
                        links = list(links)
                    # Links_list = Links_list+links

                    if len(text) > 280:
                        titles.append(title)
                        texts.append(text.strip())
                        # texts.append(
                        #     "This document is about the following topic: "+title+"\n"+text)
                        emails.append(links)
                        urls.append(PageUrl)

                    TotalPageCount += 1
                except Exception as e:
                    print("Error while reading page content for ID: " + str(ContentID))
                    print(e)
                    continue

            # next loop for space content
            ReadSize = nextedjson_extract(JSONResult.json(), "size")[0]
            if ReadSize < limitsize:
                SpaceLoop = False
            else:
                StartPosition = StartPosition + limitsize
        except Exception as e:
            print("Error while reading space content in space: " + SPACEKEY)
            print(e)
            raise Exception(e)

    return {
        "title": titles,
        "text": texts,
        "emails": emails,
        "url": urls,
        "source": len(titles) * [f"confluence/{SPACEKEY}"],
    }


def form_confluence_json(SPACEKEY):
    confluence_api_key = os.getenv("CONFLUENCE_API_KEY")

    # Loading Conflence pages
    load_dotenv()
    token = confluence_api_key
    header = {
        "Authorization": "Bearer {}".format(token),
        "Content-Type": "application/json; charset=utf-8",
    }

    # Output_JSON = GetPageContent('PINE', header)
    Output_JSON = GetPageContent(SPACEKEY, header)

    return Output_JSON


if __name__ == "__main__":

    is_error = False
    try:
        print("Running Confluence scraping process...")
        df_confluence = pd.concat(
            [
                pd.DataFrame(form_confluence_json("SPACE")),
            ]
        )
        print(df_confluence.head(100))
        print("Done!")
    except Exception as e:
        print("Error with Confluence scraping: " + str(e))
        is_error = True

    if not is_error:
        try:

            # Creating a copy of the dataframe to be uploaded to the database
            print("Creating a copy of the dataframe to be uploaded to the database...")
            output = df_confluence.copy().reset_index(drop=True)

            # Truncating output tables
            table_schema = str(sys.argv[1])
            table_name = str(sys.argv[2])
            tb = str(table_schema) + "." + str(table_name)

            print("Truncating output tables...")
            truncate_table(tb, "pg")

            # Uploading data to output tables
            print("Loading output to Database...")
            output["load_datetime"] = datetime.now()
            output["user_id"] = "Airflow"
            output = output[
                ["user_id", "title", "text", "emails", "url", "source", "load_datetime"]
            ]

            upload_data(output, "pg", tb, if_exists="append")

            print("Done!")

        except Exception as e:
            print("Error uploading data to DB: " + str(e))
