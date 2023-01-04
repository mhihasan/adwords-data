import asyncio
import os
import time

from dotenv import load_dotenv

from elasticsearch import AsyncElasticsearch

from utils import TERMS, write_to_file

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

es = AsyncElasticsearch(
    hosts=[f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"],
    verify_certs=False,
    http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
)


async def search_adwords_keywords(
    term, columns, search_type="broad", total_keywords=1000
):
    if search_type == "phrase":
        match = {"match_phrase": {"keyword": term}}
    else:
        match = {"match": {"keyword": {"query": term, "operator": "and"}}}

    resp = await es.search(
        index="adwords_en_us_2022_12",
        body={
            "sort": [{"volume": "desc"}],
            "query": {
                "bool": {
                    "must": [match, {"exists": {"field": "volume"}}],
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


async def run(terms, search_types, add_suffix, project):
    for term in terms:
        for search_type in search_types:
            print(f"<<<<<<<<< Search type: {search_type}, term: {term} >>>>>>>>>")
            t1 = time.perf_counter()
            result = await search_adwords_keywords(
                term, ["keyword", "volume"], search_type=search_type
            )
            print(f"Time taken, {term}: {time.perf_counter() - t1}")

            file_name = f"elastic/{project}/{term}_{search_type}" if not add_suffix else f"elastic/{project}/{term}"
            write_to_file(file_name, result)


async def main(project='papi'):
    search_types = ["phrase", "broad"] if project == 'dapi' else ["broad"]
    add_suffix = project == 'dapi'

    await asyncio.gather(
        run(TERMS["singe_word_terms"], search_types, add_suffix, project),
        run(TERMS["two_word_terms"], search_types, add_suffix, project),
        run(TERMS["three_word_terms"], search_types, add_suffix, project),
    )

    await es.close()


if __name__ == "__main__":
    asyncio.run(main())
