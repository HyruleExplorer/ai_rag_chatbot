RAG powered AI chatbot, using:
- OpenAI API
- LangChain
- Meta's FAISS Vector Database
- NodeJS + ExpressJS
- Confluence API
- Amazon S3
- Docker
- Airflow

The Airflow DAGs will download documents from a Confluence URL, using the official Confluence API, and then create a Vector Database in FAISS DB, one row per document.
Then, the database will be pickled and saved in a S3 bucket.
The running docker image backend code will then load this pickled file, and read it with FAISS semantic search features, helped by LangChain functions.
There are then APIs to ingest a prompt by the user, run the semantic search on the VDB, create a prompt with the user's query, enriched with the context from the top k results from the db, send the prompt with context to the LLM API, retrieve the answer, and send it back to the user.