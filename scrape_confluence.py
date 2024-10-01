import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import json
import pandas as pd
import re


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
    # SPACEKEY = 'PINE'
    limitsize = 100
    StartPosition = 0
    SpaceLoop = True
    TotalPageCount = 0

    titles, texts, emails, urls = [], [], [], []

    while SpaceLoop:
        APISPACE = "https://confluence.com/rest/api/space/{}/content/page?limit={}&start={}".format(
            SPACEKEY, limitsize, StartPosition
        )
        JSONResult = requests.get(APISPACE, headers=header, verify=False)

        # loop drop page read
        ContentIDList = nextedjson_extract(JSONResult.json(), "id")
        for ContentID in ContentIDList:
            APIpageContent = (
                "https://confluence.com/rest/api/content/{}?expand=body.storage".format(
                    ContentID
                )
            )
            PageJSONResult = requests.get(APIpageContent, headers=header, verify=False)

            PageUrl = PageJSONResult.json()["_links"]
            PageUrl = PageUrl["base"] + PageUrl["webui"]

            PageTitle = nextedjson_extract(PageJSONResult.json(), "title")[0]
            PageContent = nextedjson_extract(PageJSONResult.json(), "value")[0]

            table_regex = r"<table(.*?)table>"
            strikeout_regex = r"<s\b[^>]*>(.*?)<\/s>"

            # processing tables
            tables_str, tables_error = [], []
            tables = re.findall(table_regex, PageContent)

            page_processed = PageContent

            if tables != []:
                for table in tables:
                    try:
                        tables_str.append(html_to_text("<table" + table + "table>"))
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
                texts.append(text)
                # texts.append(
                #     "This document is about the following topic: "+title+"\n"+text)
                emails.append(links)
                urls.append(PageUrl)

            TotalPageCount += 1

        # next loop for space content
        ReadSize = nextedjson_extract(JSONResult.json(), "size")[0]
        if ReadSize < limitsize:
            SpaceLoop = False
        else:
            StartPosition = StartPosition + limitsize

    return {"title": titles, "text": texts, "emails": emails, "url": urls}


def scrape_confluence():
    confluence_api_key = os.getenv("CONFLUENCE_API_KEY")

    # Loading Conflence pages
    load_dotenv()
    token = confluence_api_key
    header = {
        "Authorization": "Bearer {}".format(token),
        "Content-Type": "application/json; charset=utf-8",
    }

    Output_JSON = GetPageContent("PINE", header)

    with open("./confluence.json", "w") as fout:
        json.dump(Output_JSON, fout)


if __name__ == "__main__":
    scrape_confluence()
    print("Scrape Confluence completed.")
