import json
import sys
import os
from db_functions import execute_sql

if __name__ == "__main__":
    try:
        human_score = ""
        qa_id = ""
        user_id = ""

        human_score = sys.argv[1]
        qa_id = sys.argv[2]
        user_id = sys.argv[3]
        human_score = human_score.replace('"', "'")
        qa_id = qa_id.replace('"', "'")
        user_id = user_id.replace('"', "'")

        print(f"chat_v4.py ---- MAIN - human_score: {human_score}")
        print(f"chat_v4.py ---- MAIN - qa_id: {qa_id}")
        print(f"chat_v4.py ---- MAIN - user_id: {user_id}")

        qa_table = str(os.getenv("QA_TB"))

        query = (
            f"""UPDATE {qa_table} SET h_score = {human_score} WHERE qa_id = {qa_id};"""
        )

        execute_sql("pg", query)

        result = f"Score: {human_score} uploaded for QA: {qa_id}."
        print(result)

    except Exception as e:
        result = f"Error!: {str(e)}"
        print(result)
