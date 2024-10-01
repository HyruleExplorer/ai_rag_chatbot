import json
import os
import pickle
import re
import sys
import time
from datetime import datetime
from urllib.parse import unquote
import pandas as pd
from langchain_community.vectorstores import FAISS

from utils import (
    contains_word,
    count_tokens,
    create_final_prompt_for_challenger,
    encode_json_string,
    extract_answer_from_query_llm,
    extract_key_words,
    query_llm,
    query_llm_challenger,
    remove_special_chars,
    upload_qa_to_db,
    separate_query_and_hastag,
    find_title_matched_with_hashtags,
)

MAX_TOKENS = 4000
DOC_AMOUNT = 6
MODEL = "llama-2-70b-chat"
DOC_FILE_NAME = "docs.json"
FILE_NAME = "confluence_vdb"


def chat(json_data, secret_key, ddc_api_key, verbose=True):
    # opening the file where we stored the pickled db
    file = open(FILE_NAME, "rb")
    # loading information from the db
    db = pickle.load(file)
    # close the file
    file.close()
    # Loading information from the json file
    file = open(DOC_FILE_NAME)
    confluence_json = json.load(file)
    file.close()

    # Loading User's query and chat history
    received_json_string = json_data
    received_json = json.loads(received_json_string)
    print(f"chat_v4.py  ---- CHAT LOG - recived_json: {received_json}")

    chat_history = received_json["chat_history"]
    if chat_history:
        chat_history = chat_history.replace("USER: ", "\\nUSER: ")
        chat_history = chat_history.replace("AI: ", "\\nAI: ")
        chat_history = chat_history + "\\n"
        print("chat_v4.py ---- CHAT LOG - CHAT_HISTORY: " + chat_history)

    query_st = received_json["query"]
    if verbose:
        print("chat_v4.py ---- CHAT LOG - QUERY STRING: " + query_st)

    # Separating hashtags from the query
    full_query = separate_query_and_hastag(query_st)

    hashtags = full_query["hashtag"]
    query = full_query["query"]

    if verbose:
        print("chat_v4.py ---- CHAT LOG - QUERY: " + query)

    # Creating a historied query to store the user's past queries in the prompt
    query_historied = ""
    if chat_history != "":
        pattern = r"USER: (.+?)\\n"
        USER_queries = re.findall(pattern, chat_history)
        query_historied = "\n".join(USER_queries) + "\n" + query
    if verbose:
        print("chat_v4.py ---- CHAT LOG - QUERY HISTORIED: " + query_historied)

    if query_historied != "":
        chat_text = query_historied
    else:
        chat_text = query

    # If there are hashtags, we use the similarity search with score to get all documents from the db and the corresponding scores
    if len(hashtags):
        docs = db.similarity_search_with_score(
            chat_text, k=100000, fetch_k=100000, score_threshold=100
        )
    # If there are no hashtags, we use the basic similarity search function to get only the top k documents from the db
    else:
        docs = db.similarity_search(chat_text, k=DOC_AMOUNT)

    if verbose:
        print(
            f"chat_v4.py ---- CHAT LOG - AMOUNT OF DOCS RETRIEVED FROM DB: {len(docs)}"
        )

    # Extracting keywords from the chat text
    keywords_storage = extract_key_words(chat_text)
    # Adding keywords from the historized query, if the word contains "_".
    # This is to ensure that column names are also considered as keywords
    for word in chat_text.split(" "):
        if "_" in word and word not in keywords_storage:
            keywords_storage.append(word)

    if verbose:
        for word in keywords_storage:
            print(f"chat_v4.py ---- CHAT LOG - KEYWORD: {word}")

    # Filtering out the documents that don't contain the keywords in their content
    if len(hashtags):
        docs = [
            doc
            for doc in docs
            # If we used the similarity search with score, the documents are stored in a tuple with the score
            if contains_word(doc[0].page_content.lower(), keywords_storage)
        ]
    else:
        docs = [
            doc
            for doc in docs
            # If we used the basic similarity search, the documents are stored in a list
            if contains_word(doc.page_content.lower(), keywords_storage)
        ]

    if verbose:
        print(
            f"chat_v4.py ---- CHAT LOG - AMOUNT OF DOCS AFTER KEYWORDS FILTER: {len(docs)}"
        )

    # If there are hashtags, we filter out the documents that don't contain the hashtags in their title
    if len(hashtags):
        docs = [
            doc[0] for doc in docs if find_title_matched_with_hashtags(hashtags, doc[0])
        ]
        docs = [doc for doc in docs[:DOC_AMOUNT]]
        if verbose:
            print(
                f"chat_v4.py ---- CHAT LOG - AMOUNT OF DOCS AFTER TITLE MATCH WITH HASHTAG: {len(docs)}"
            )

    # In case there are no documents with the keywords, we return a default message
    if not docs:
        data = {
            "query": query,
            "answer": "Sorry, I cannot answer your query based on the information found in the documentation. Can you please provide more context?",
            "excerpts": [],
        }
        json_string = json.dumps(data, indent=4)

        return json_string

    # Preparing context for prompt
    context_text = []
    for doc in docs:
        context_text.append(str(doc.page_content).replace('"', "-"))

    # Generating prompt with the context & chat history
    prompt_with_context = create_final_prompt_for_llm(
        chat_history,
        context_text,
        query,
        context_pieces=DOC_AMOUNT,
        max_tokens=MAX_TOKENS,
    )

    # Preparing contextual documents for the LLM
    excerpts = []
    for doc in docs:
        idx = doc.metadata["seq_num"] - 1
        cur_doc_text = str(doc.page_content).replace('"', "-")
        cur_doc_title = str(confluence_json["title"][idx])
        cur_doc_emails = str(confluence_json["emails"][idx])
        cur_doc_url = str(confluence_json["url"][idx])
        # Creating context for the LLM
        context_text.append(cur_doc_text)
        # Creating excerpts that will be sent to the user
        excerpts.append(
            {
                "doc_title": cur_doc_title,
                "doc_url": cur_doc_url,
                "doc_emails": cur_doc_emails,
                "doc_content": cur_doc_text.replace("{", "|").replace("}", "|"),
            }
        )

    if verbose:
        print("chat_v4.py ---- CHAT LOG - PROMPT WITH CONTEXT: " + prompt_with_context)
        print(
            f"chat_v4.py ---- CHAT LOG - N TOKENS IN PROMPT: {count_tokens(prompt_with_context)}"
        )

    # print("======================== FULL PROMPT: \n" + prompt_with_context + "\n==========================\n")

    # Running LLM. If a particular error shows up we retry up to 2 times with 2 seconds delay between each attempt:
    answer = ""
    error = ""
    trial = 0
    for attempt in range(3):
        try:
            trial += 1
            # Calling the LLM
            if trial == 1:
                model_name = MODEL
            elif trial == 2:
                model_name = "mixtral-8x7b-instruct-v01"
            elif trial == 3:
                model_name = "gemma-7b-it"
            answer = query_llm_challenger(
                prompt_with_context, ddc_api_key, model_name=model_name
            )
            answer = answer.strip()
            print(f"\nchat_v4.py ---- CHAT LOG - Trial: {str(trial)}")
            print(f"chat_v4.py ---- CHAT LOG - Model: {model_name}")
            print(f"chat_v4.py ---- CHAT LOG - DDC API LLM's answer is >>>>>> {answer}")
            # Exiting the loop
            if answer != "":
                error = ""
                break
        except Exception as e:
            error = e
            print(f"\nchat_v4.py ---- CHAT LOG - ERROR: {error}")
            # Sleeping one second before retrying to call the LLM:
            time.sleep(2)

    if error != "":
        answer = f"I'm sorry, there was an internal system error. Can you repeat your query?\n\n======== ERROR: {error}\n========"
    elif answer == "":
        answer = f"I'm sorry, there was an internal issue. Can you repeat your query?"
    answer = encode_json_string(answer)
    print(f"chat_v4.py ---- CHAT LOG - Encoded answer: {answer}")

    # Creating the dictionary to send to the user
    data = {"query": query, "answer": answer, "excerpts": excerpts}

    # Converting the dictionary to a JSON string
    json_string = json.dumps(data, indent=4)
    # Print the JSON string
    # formatted_json_string = json.loads(json_string)
    # print(json.dumps(formatted_json_string, indent=4))
    # print(answer + "\n\nRelevant documentation found in Confluence:\n\n"+context_text)

    # Returning json to the user
    print(f"chat_v4.py ---- CHAT LOG - json_string: {json_string}")
    return json_string


if __name__ == "__main__":
    try:
        secret_key = os.getenv("SECRET_KEY")
        ddc_api_key = os.getenv("DDC_API_KEY")

        chat_history = ""
        new_query = ""
        answer = ""

        user_query = sys.argv[1]
        chat_history = sys.argv[2]
        conversation_id = sys.argv[3]
        qa_id = sys.argv[4]
        user_id = sys.argv[5]
        user_id = user_id.replace('"', "")
        conversation_id = conversation_id.replace('"', "")
        qa_id = qa_id.replace('"', "")

        chat_history = unquote(chat_history)
        new_query = remove_special_chars(user_query)

        print(f"chat_v4.py ---- MAIN - user_query: {new_query}")
        print(f"chat_v4.py ---- MAIN - chat_history: {chat_history}")
        print(f"chat_v4.py ---- MAIN - conversation_id: {conversation_id}")
        print(f"chat_v4.py ---- MAIN - qa_id: {qa_id}")
        print(f"chat_v4.py ---- MAIN - user_id: {user_id}")

        # Keeping chat history to the last 5 iterations
        if len(chat_history.split("USER")) > 6:
            chat_array = chat_history.split("USER")
            chat_array.pop(1)
            chat_history = "USER".join(chat_array)
        chat_history = remove_special_chars(chat_history)

        json_string_to_send = f"""{{
            "chat_history" : "{chat_history}",
            "query": "{new_query}"
        }}"""

        # ======================= Calling CHAT API
        json_string = chat(json_string_to_send, secret_key, ddc_api_key, verbose=True)

        # Getting data from the CHAT API and displaying answer
        data = json.loads(json_string)
        query = data["query"]
        print(f"chat_v4.py ---- MAIN - query: {query}")
        answer = data["answer"]
        print(f"chat_v4.py ---- MAIN - answer: {answer}")

        # Removing sentences which contain http patterns
        # answer = re.sub('http\S*', '', answer)
        # answer = answer.replace('URL', '').replace('url', '')
        answer_arr = answer.split(".")
        for ans in answer_arr:
            # if (' url' in ans.lower() or 'documentation' in ans.lower() or '##' in ans.lower()):
            if "##" in ans.lower():
                answer_arr.remove(ans)
        answer = ".".join(answer_arr)

        # Getting only first 12 sentences in the answer (to avoid extremely long answers which indicate higher chance of hallucination)
        answer_arr = answer.split(".")
        answer_arr_new = []
        if "1." not in answer:
            for ans in answer_arr[:10]:
                answer_arr_new.append(ans)
            answer = ".".join(answer_arr_new)

        docs_array = data["excerpts"]
        # Removing first and last single quote if present
        if answer[0] == "'":
            answer = answer[1:]
        answer_l = len(answer) - 1
        if answer[answer_l] == "'":
            answer = answer[:answer_l]

        # Adding links to the answer
        answer = (
            answer
            + "<br/>You can find more information in Confluence at these links:<br/>"
        )
        checked_docs = []
        for doc in docs_array:
            doc_title = doc.get("doc_title")
            doc_url = doc.get("doc_url")
            # print(doc_url)

            if doc_title not in checked_docs:
                checked_docs.append(doc_title)
                answer = (
                    answer + f"<br/><a href='{doc_url}' target='_blank'>{doc_title}</a>"
                )

                doc_emails = (
                    doc.get("doc_emails")
                    .replace("[", "")
                    .replace("]", "")
                    .replace('"', "")
                    .replace("'", "")
                    .replace(" ", "")
                )
                # print(doc_emails)

                if doc_emails != "":
                    doc_emails_arr = doc_emails.split(",")
                    # Removing duplicate emails
                    doc_emails_arr = list(set(doc_emails_arr))
                    # print(doc_emails_arr)
                    if len(doc_emails_arr) > 0:
                        # print("---- Email contacts:")
                        answer = answer + f"<br/><div>Email contacts:</div>"
                        # for email in doc_emails_arr:
                        for idx, email in enumerate(doc_emails_arr):
                            answer = (
                                answer
                                + f"<a style='position: relative; left: 42px;' href='mailto: {email}'>{email}</a><br/>"
                            )
                            if idx == len(doc_emails_arr) - 1:
                                answer = answer + "<br/>"
                            # print(email)

        if "\\" in answer:
            answer = answer.replace("\\", "\\\\")
        print(
            f"chat_v4.py ---- MAIN - FINAL ANSWER BEFORE JSON LOADING AND DUMPING: {answer}"
        )

        # Uploading Q and A to Database
        qa_data = {
            "user_id": user_id,
            "question": query,
            "answer": answer,
            "conversation_id": conversation_id,
            "qa_id": qa_id,
            "load_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        qa_df = pd.DataFrame(data=[qa_data])
        upload_qa_to_db(qa_df)

        answer = f"""{{"answer" : "{answer}"}}"""
        answer_json = json.loads(answer)
        answer_dump = json.dumps(answer_json)
        print(answer_dump)

    except Exception as e:
        answer = f"""{{"answer" : "Error!: {str(e)}"}}"""
        answer_json = json.loads(answer)
        answer_dump = json.dumps(answer_json)
        print(answer_dump)
