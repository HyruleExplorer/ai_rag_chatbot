import os
import re
import ray
import json
import yake
import openai
import tiktoken
import requests
from openai import OpenAI
import httpx

# from google.cloud import aiplatform
# from vertexai.preview.language_models import TextGenerationModel
from db_functions import *


def encode_json_string(json_string):
    json_string = (
        json_string.encode("utf-8")
        .decode("utf-8", "ignore")
        .replace("\\n", "<br />")
        .replace("\\n", "<br/>")
        .replace("\n", "<br/>")
        .replace("\t", " ")
        .replace("\\b", "")
        .replace("\\t", " ")
        .replace("\\f", "")
        .replace("\\r", " ")
        .replace("??", "?")
        .replace("*", "")
        .replace("’", "'")
        .replace("‘", "'")
        .replace('"', "'")
        .replace("`", "'")
        .replace("“", "")
        .replace("”", "'")
        .replace("\\u", "")
        .replace("\\\\", "\\")
    )
    return json_string


def remove_special_chars(text):
    text = (
        text.replace('"', "'")
        .replace("{", "")
        .replace("}", "")
        .replace("[", "")
        .replace("]", "")
        .replace("<", "")
        .replace(">", "")
        .replace("??", "?")
        .replace("*", "")
        .replace("’", "'")
        .replace("‘", "'")
        .replace("'", "")
        .replace('"', "")
        .replace("`", "")
        .replace("“", "")
        .replace("”", "'")
        .replace("#", "")
        .replace("\\\\", "\\")
    )
    return text


def upload_qa_to_db(df):
    qa_table = str(os.getenv("QA_TB"))
    upload_data(df, "pg", qa_table, "append")


def contains_word(string, word_list):
    """Check if string contains at least one string from the list (as a single word)"""
    # Create a regular expression pattern to match whole words
    pattern = r"\b(?:" + "|".join(map(re.escape, word_list)) + r")\b"

    # Perform the search using the pattern
    match = re.search(pattern, string, flags=re.IGNORECASE)

    return match is not None


def extract_key_words(text):
    """Extract keywords from text"""
    # Set up keywords extractor
    kw_extractor = yake.KeywordExtractor()
    custom_kw_extractor = yake.KeywordExtractor(
        lan="en", n=3, dedupLim=1, top=7, features=None
    )

    # Extract keywords and return as list
    keywords = custom_kw_extractor.extract_keywords(text)
    keywords_storage = []
    for keyword in keywords:
        keywords_storage.append(keyword[1].lower())

    return keywords_storage


def count_tokens(prompt):
    """Count amount of tokens in the prompt"""
    encoding = tiktoken.encoding_for_model("gpt2")
    encoded_prompt = encoding.encode(encode_json_string(prompt))
    return len(encoded_prompt)


# def create_final_prompt(
#     chat_history, context_text, query, context_pieces=10, max_tokens=2020
# ):
#     """Creation of final prompt - if prompt length > max_tokens remove few sentences from AI answer and one similar document"""
#     prompt = create_prompt(chat_history, context_text, query, context_pieces)
#     print(
#         "******************* UTILS - Creating final prompt. Count of tokens: "
#         + str(count_tokens(prompt))
#     )
#     while count_tokens(prompt) > max_tokens:
#         # if chat_history is not null, remove last AI sentence
#         if chat_history != "":
#             print(
#                 "******************* UTILS - Too many tokens, shortening chat history by removing last AI sentence from each answer..."
#             )
#             chat_history = shorten_chat_history(chat_history)
#         # Remove the least similar context piece
#         if count_tokens(prompt) > max_tokens:
#             print(
#                 "******************* UTILS - Too many tokens, shortening prompt by removing last sentence from last document in the context..."
#             )
#             # context_pieces -= 1
#             context_text = shorten_context(context_text)
#         # Recreating the prompt
#         prompt = create_prompt(chat_history, context_text, query, context_pieces)
#     return prompt


def create_final_prompt_for_llm(
    chat_history, context_text, query, context_pieces=10, max_tokens=2020
):
    """Creation of final prompt - if prompt length > max_tokens remove few sentences from AI answer and one similar document"""
    prompt = create_prompt_for_llm(chat_history, context_text, query, context_pieces)
    print(
        "******************* UTILS - Creating final prompt. Count of tokens: "
        + str(count_tokens(prompt))
    )
    while count_tokens(prompt) > max_tokens:
        # if chat_history is not null, remove last AI sentence
        if chat_history != "":
            print(
                "******************* UTILS - Too many tokens, shortening chat history by removing last AI sentence from each answer..."
            )
            chat_history = shorten_chat_history(chat_history)
        # Remove the least similar context piece
        if count_tokens(prompt) > max_tokens:
            print(
                "******************* UTILS - Too many tokens, shortening prompt by removing last sentence from last document in the context..."
            )
            # context_pieces -= 1
            context_text = shorten_context(context_text)
        # Recreating the prompt
        prompt = create_prompt_for_llm(
            chat_history, context_text, query, context_pieces
        )
    return prompt


def stream_and_yield_response(response):
    for chunk in response.iter_lines():
        decoded_chunk = chunk.decode("utf-8")
        if decoded_chunk == "data: [DONE]":
            pass
        elif decoded_chunk.startswith("data: {"):
            payload = decoded_chunk.lstrip("data:")
            json_payload = json.loads(payload)
            yield json_payload["choices"][0]["text"]


def separate_query_and_hastag(query_st):
    # Regular expression to match query_strings starting with '-'
    pattern = r"\s*(-\w+)\s*"

    # Find all matches of the pattern in the string
    matches = re.findall(pattern, query_st)

    # Remove the matched substrings from the original string
    for match in matches:
        query_st = query_st.replace(match, "")

    # Split the remaining string by whitespace to get parts
    hashtag = [match[1:] for match in matches]
    query = " ".join(query_st.split())

    hashtag_query_dict = {"query": query, "hashtag": hashtag}
    return hashtag_query_dict


def find_title_matched_with_hashtags(hashtags, doc):
    status = True
    for hashtag in hashtags:
        if hashtag.lower() not in doc.metadata["page_name"].lower():
            status = False
    return status


def query_llm(instruction, api_key, model_name="zephyr-7b-beta"):
    """
    Creates a request to LLM model with API key in header.
    """

    full_answer = ""
    url = ""
    # payload = {"instruction": instruction}
    headers = {
        "accept": "application/json",
        "api-key": api_key,
        "Content-Type": "application/json",
    }
    data = {
        "prompt": f"{instruction}",
        "temperature": 0.5,
        "top_p": 0.95,
        "max_tokens": "500",
        "stream": True,
        "model": f"{model_name}",
    }

    try:
        response = requests.post(
            url, headers=headers, json=data, stream=data["stream"], verify=False
        )
        response.raise_for_status()

        if data["stream"]:
            for result in stream_and_yield_response(response):
                print(result, end="")
                full_answer = full_answer + result
        else:
            response_dict = response.json()
            result = response_dict["choices"][0]["text"]
            print(result)
            full_answer = result

        return full_answer

    except requests.exceptions.HTTPError as err:
        print("Error code:", err.response.status_code)
        print("Error message:", err.response.text)
        return "Error during prompt processing: " + str(err.response.text)
    except Exception as err:
        print("Error:", err)
        return "Error during prompt processing: " + str(err)


#########################################################################
#########################################################################
######################### LLM API ################################
#########################################################################
#########################################################################
http_client = httpx.Client(verify=False)


def query_llm_challenger(instruction, api_key, model_name="llama-2-70b-chat"):
    client = OpenAI(
        base_url="",
        http_client=http_client,
        api_key=api_key,
    )

    full_answer = ""
    # Available Models list
    # available_models = [
    #     "mixtral-8x7b-instruct-v01",
    #     "llamaguard-7b",
    #     "gemma-7b-it",
    #     "mistral-7b-instruct-v02",
    #     "phi-2",
    #     "llama-2-70b-chat",
    #     "phi-3-mini-128k-instruct",
    #     "llama-3-8b-instruct",
    # ]

    completion = client.completions.create(
        model=model_name,
        max_tokens=500,
        prompt=instruction,
        stream=False,
    )

    print(completion.choices[0].text)
    full_answer = completion.choices[0].text

    return full_answer


#########################################################################
#########################################################################


def extract_answer_from_query_llm(input_string):
    """falcon-40b-instruct returning the full prompt feeded inside and pasting the answer aftre word "ANSWER:" """
    # Find the position of "ANSWER: " in the input_string
    answer_index = input_string.rfind("ANSWER: ")
    if answer_index == -1:
        # "ANSWER: " not found in the string, return an empty string
        return ""
    # Extract the part of the text after "ANSWER: "
    extracted_text = input_string[answer_index + len("ANSWER: ") :].strip()
    return extracted_text


def shorten_chat_history(chat_history):
    """Remove last sentence from AI answers in the chat history"""

    # Extracting USER queries with regex
    pattern = r"USER: (.+?)\\n"
    USER_queries = re.findall(pattern, chat_history)
    # print("************ UTILS - Shortening chat history. USER_queries: ")
    # print(USER_queries)

    # Extracting AI answers with regex
    pattern = r"AI: (.+?)\\n"
    AI_answers = re.findall(pattern, chat_history)
    # print("************ UTILS - Shortening chat history. AI_answers: ")
    # print(AI_answers)

    # Deleting last sentence from each AI answer
    AI_answers_new = []
    for ans in AI_answers:
        regex_pattern = r"[?.!]"
        ans_arr = re.split(regex_pattern, ans)
        # print("************ UTILS - Shortening AI answer. BEFORE: " + ans)
        # Checking if there is at least one answer
        if len(ans_arr) > 2:
            # Deleting the last sentence.
            ans_arr.pop(-1)
        ans = ".".join(ans_arr)
        ans = ans.strip()
        # print("************ UTILS - Shortening AI answer. AFTER: " + ans)
        AI_answers_new.append(ans)

    # Recreating new chat history string
    chats = []
    for idx, q in enumerate(USER_queries):
        # print(idx, q)
        chat = "USER: " + q + "\\nAI: " + AI_answers_new[idx] + "\\n"
        chats.append(chat)
    chat_history = "".join(chats)
    chat_history = chat_history.replace("\n", "\\n").replace('"', "'")

    return chat_history


def shorten_context(context):
    """Remove last sentence from the least similar context piece"""
    # Take last context piece if its not empty
    if len(context[-1]) > 0:
        regex_pattern = r"[?.!]"
        context_arr = re.split(regex_pattern, context[-1])
        # If length of the last piece > 1 then remove one sentence
        if len(context_arr) > 1:
            context_arr.pop(-1)
            context_arr = ".".join(context_arr)
            context_arr = context_arr.strip()
            context[-1] = context_arr
        else:
            # If piece consists of only one sentence remove it completely
            context.pop(-1)
    else:
        # Remove piece if its empty
        context.pop(-1)

    return context


def shorten_prompt(prompt, max_tokens=1024):
    """Remove tokens exceeding max_tokens"""
    encoding = tiktoken.encoding_for_model("gpt2")
    encoded_prompt = encoding.encode(prompt)
    while len(encoded_prompt) > max_tokens:
        prompt = prompt.rsplit(" ", 1)[0]
        encoded_prompt = encoding.encode(prompt)
    return prompt


def create_prompt_for_llm(chat_history, context_text, query, context_pieces):
    """Create final prompt"""
    prompt_with_context = ""
    if chat_history != "":
        prompt_with_context = f"""<s>[INST] <<SYS>>\n
YOU ARE A HELPFUL AI ASSISTANT. BELOW IS AN INSTRUCTION 
THAT DESCRIBE A TASK. WRITE A RESPONSE THAT APPROPRIATELY COMPLETES THE REQUEST. IF THE TASK
CANNOT BE RESPONSDED USING THE DOCUMENTATION, REPLY WITH "I DON'T KNOW". YOUR RESPONSE SHOULD 
BE COMPLETE AND DETAILED.
\n<</SYS>>\n\nContext:\n
{'. '.join(context_text[:context_pieces])}

\n
HERE ARE PAST INTERACTIONS BETWEEN YOU AND THE USER:
{chat_history}

\n\nQuestion:\n
{query}
[/INST]
"""
    else:
        prompt_with_context = f"""<s>[INST] <<SYS>>\n
YOU ARE A HELPFUL AI ASSISTANT. BELOW IS AN INSTRUCTION 
THAT DESCRIBE A TASK. WRITE A RESPONSE THAT APPROPRIATELY COMPLETES THE REQUEST. IF THE TASK
CANNOT BE RESPONSDED USING THE DOCUMENTATION, REPLY WITH "I DON'T KNOW". YOUR RESPONSE SHOULD 
BE COMPLETE AND DETAILED.
\n<</SYS>>\n\nContext:\n
{'. '.join(context_text[:context_pieces])}
\n\nQuestion:\n
{query}
[/INST]
"""
    return prompt_with_context


def init_openai():
    openai.api_key = os.getenv("OPENAI_API_KEY")
    openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
    openai.api_type = "azure"
    openai.api_version = os.getenv("OPENAI_API_VERSION")
    gpt_model = "gpt-35-turbo"
    kernel = Kernel()

    # create .env file
    file = open(".env", "w")
    file.write(
        f'AZURE_OPENAI_DEPLOYMENT_NAME="{os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")}"\n'
    )
    file.write(f'AZURE_OPENAI_ENDPOINT="{os.getenv("AZURE_OPENAI_ENDPOINT")}"\n')
    file.write(f'AZURE_OPENAI_API_KEY="{os.getenv("AZURE_OPENAI_API_KEY")}"\n')
    file.close()

    deployment, api_key, endpoint = azure_openai_settings_from_dot_env()
    kernel.add_chat_service(
        "chat-gpt",
        AzureChatCompletion(
            deployment_name=deployment, endpoint=endpoint, api_key=api_key
        ),
    )
    connector = BingConnector(api_key=os.getenv("BING_SUBSCRIPTION_KEY"))
    web_skill = kernel.import_plugin(
        plugin_instance=WebSearchEnginePlugin(connector),
        plugin_name="WebSearch",
    )

    return kernel, connector, web_skill


def query_openai(prompt, kernel, connector, web_skill):

    qna = kernel.create_semantic_function(prompt, temperature=0.0)
    context = kernel.create_new_context()
    context["num_results"] = "20"
    context["offset"] = "0"
    result = qna.invoke(context=context)

    return result
