import argparse
import asyncio
import json
import logging
import os.path
import time
from datetime import datetime
from pathlib import Path

import asyncpg
import boto3
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dotenv_path = os.path.join(Path(__file__).parent.parent, ".env")
print("d", dotenv_path)
load_dotenv(dotenv_path)


db_params = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

load_type_config = {
    "test": 2,
    "small": 120,
    "medium": 360,
    "large": 850,
}


def read_queries(file_name="topics.json"):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path, "r") as f:
        data = json.loads(f.read())

    return [
        dict(
            term=item.get("term"),
            country=item.get("country"),
            language=item.get("language"),
            domain="blog.xxx.com",
        )
        for item in data
    ]


def chunks(terms, chunk_size=5000):
    for i in range(0, len(terms), chunk_size):
        yield terms[i : i + chunk_size]


def convert_term_to_tsvector_for_search(term, opr="&"):
    split_term = term.split(" ")
    return opr.join(split_term)


def ts_to_date(ts):
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def time_tracker(func):
    async def _func(term, **kwargs):
        start_time = time.time()
        results, error = await func(term, **kwargs)
        return (
            term,
            results,
            error,
            (time.time() - start_time),
            ts_to_date(start_time),
            ts_to_date(time.time()),
        )

    return _func


@time_tracker
async def fetch_from_postgresql(term, opr="&", pool=None):
    if not pool:
        raise Exception("Connection pool not provided")

    term = convert_term_to_tsvector_for_search(term, opr)
    result = None
    is_error = True

    try:
        query = f"""
            with matched_keywords as (
                select *
                from adwords_en_us
                where keyword_tsv @@ to_tsquery('{term}')
            )
            select keyword, volume
            from matched_keywords
            where spell_type is null
            order by volume desc
            limit 1000;
        """
        async with pool.acquire() as con:
            result = await con.fetch(query)
            result = len(result)
            is_error = False
    except Exception as error:
        logger.error(str(error))

    return result, is_error


es = AsyncElasticsearch(hosts=[f'http://{os.getenv("DB_HOST")}:9201'], verify_certs=False,
                                http_auth=('elastic', 'RTfk38CuXJ8EwDNHTMUCqCue2Xa5ePuX'))


@time_tracker
async def fetch_from_elasticsearch(term):
    try:
        resp = await es.search(
            index="adwords_en_us_2022_12",
            body={
                "sort": [
                    {"volume": "desc"}
                ],
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "keyword": {
                                        "query": term,
                                        "operator": "AND"
                                    }
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "exists": {
                                    "field": "spell_type"
                                }
                            }
                        ]
                    }
                }
            },
            size=1000,
        )
        is_error = False
    except Exception as e:
        logger.error(e)
        resp = {}
    finally:
        await es.close()
    # logger.info(resp)
    results = []
    for hit in resp.get('hits', {}).get('hits', []):
        src = hit['_source']
        results.append({"keyword": src['keyword'], "volume": src['volume']})

    write_to_file(results, term)



sources = {
    "postgresql": fetch_from_postgresql,
}


def save_stats(results, load_type, source):
    # Prepare file names
    file_name_prefix = f"{source}_{load_type}"
    results_file_name = f"{file_name_prefix}_results.csv"
    stats_file_name = f"{file_name_prefix}_stats.csv"

    # Save results and stats to csv file
    df = pd.DataFrame(results)
    df.to_csv(results_file_name, index=False)
    df['time_took'].describe().to_csv(stats_file_name)

    # Upload to s3
    s3 = boto3.resource("s3")
    for f in [results_file_name, stats_file_name]:
        s3.meta.client.upload_file(f, os.getenv("BUCKET_NAME"), f"load_tests/{f}")
        os.remove(f)


async def run_test(source, load_type):
    results = []
    queries_to_run = load_type_config.get(load_type)
    queries = read_queries()
    terms = [query.get("term") for query in queries]

    pool = (
        await asyncpg.create_pool(min_size=50, max_size=queries_to_run,  **db_params)
        if source == "postgresql"
        else None
    )

    logger.info(f"Processing for: {load_type} from source: {source}")
    for chunk_terms in chunks(terms, queries_to_run):
        logger.info(f"Processing {len(chunk_terms)} terms")
        tasks = []
        for term in chunk_terms:
            tasks.append(sources.get(source)(term, pool=pool))
        logger.info(f"Task Build {len(chunk_terms)}")
        result = await asyncio.gather(*tasks)

        for r in result:
            results.append(
                dict(
                    term=r[0],
                    result=r[1],
                    error=r[2],
                    time_took=r[3],
                    request_start_time=r[4],
                    request_end_time=r[5],
                )
            )
    save_stats(results, load_type, source)
    logger.info(
        f"Load testing done for source: {source} and load_type: {load_type}"
    )


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--load_type",
        type=str,
        help="Load testing type",
        default="test",
        choices=["test", "small", "medium", "large"],
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Load testing type",
        default="postgresql",
        choices=["postgresql", "elastic_search"],
    )
    args = parser.parse_args()
    asyncio.run(run_test(args.source, args.load_type))


if __name__ == "__main__":
    """
    nohup python -m adwords_load_test --load_type small  > load-testing_small.out 2>&1 &
    """
    asyncio.run(fetch_from_elasticsearch("sams club"))
