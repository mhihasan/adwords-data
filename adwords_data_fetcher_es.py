import asyncio
import os
import time

# from dotenv import load_dotenv

# from elasticsearch import AsyncElasticsearch

from fts_elastic.es_client import get_es_client
from utils import TERMS, write_to_file

# dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
# load_dotenv(dotenv_path)
#
# ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
# ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT")
# ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
# ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")



async def search_adwords_keywords(
    es_client, term, columns, search_type="broad", total_keywords=1000
):
    if search_type == "phrase":
        match = {"match_phrase": {"keyword": term}}
    else:
        match = {"match": {"keyword": {"query": term, "operator": "and"}}}

    resp = await es_client.search(
        index="adwords_en_us_2022_12",
        body={
            "sort": [{"volume": "desc"}],
            "query": {
                "bool": {
                    "must": [match],
                    "must_not": [{"exists": {"field": "spell_type"}}],
                }
            },
            "fields": columns,
            "source": False,
        },
        size=total_keywords,
    )

    results = []
    for hit in resp.get("hits", {}).get("hits", []):
        fields = hit["fields"]
        results.append({"keyword": fields["keyword"][0], "volume": fields["volume"][0]})

    return results


async def run(es_client, terms, search_types, add_suffix, project):
    for term in terms:
        for search_type in search_types:
            print(f"<<<<<<<<< Search type: {search_type}, term: {term} >>>>>>>>>")
            t1 = time.perf_counter()
            result = await search_adwords_keywords(
                es_client, term, ["keyword", "volume"], search_type=search_type
            )
            print(f"Time taken, {term}: {time.perf_counter() - t1}")

            file_name = f"elastic_local/{project}/{term}_{search_type}" if add_suffix else f"elastic/{project}/{term}"
            write_to_file(file_name, result)


async def main(project='papi'):
    search_types = ["phrase", "broad"] if project == 'dapi' else ["broad"]
    add_suffix = project == 'dapi'

    async with get_es_client() as es_client:
        await asyncio.gather(
            run(es_client, TERMS["singe_word_terms"], search_types, add_suffix, project),
            run(es_client, TERMS["two_word_terms"], search_types, add_suffix, project),
            run(es_client, TERMS["three_word_terms"], search_types, add_suffix, project),
        )


if __name__ == "__main__":
    asyncio.run(main())
