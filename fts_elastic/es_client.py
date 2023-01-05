import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

dotenv_path = os.path.join(Path(__file__).parent.parent, ".env")
print("do", dotenv_path)
load_dotenv(dotenv_path)

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")


@asynccontextmanager
async def get_es_client():
    client = AsyncElasticsearch(
        hosts=[f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"],
        verify_certs=False,
        http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    )

    yield client

    await client.close()
