import asyncio
import csv
import os.path
from pathlib import Path

from elasticsearch.helpers import async_bulk

from fts_elastic.es_client import get_es_client
from fts_elastic.index_creator import create_index


def generate_actions(file_path, index_name):
    with open(file_path, mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            doc = {
                "keyword": row["keyword"],
                "volume": row["volume"],
            }

            yield {
                "_index": index_name,
                "_source": doc,
            }


async def main(index_name="adwords_en_us_2022_12"):
    print("Indexing documents...")
    folder_path = os.path.join(Path(__file__).parent.parent, "postgres/papi")

    docs_inserted = 0
    async with get_es_client() as es:
        should_create_index = not es.indices.exists(index_name)
        if should_create_index:
            await create_index(es, index_name)

        for file in os.listdir(folder_path):
            file_path = f'{folder_path}/{file}'

            print("file_path", file_path)

            success_docs, failed_docs = await async_bulk(es, actions=generate_actions(file_path, index_name), chunk_size=1000, raise_on_error=True)
            docs_inserted += success_docs

        print(f"Indexed {docs_inserted} documents")


if __name__ == "__main__":
    asyncio.run(main())
