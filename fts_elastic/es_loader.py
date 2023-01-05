import argparse
import asyncio
import gzip
from contextlib import asynccontextmanager
from functools import lru_cache
from io import BytesIO
from typing import Dict, List

import aioboto3
import boto3
import pandas as pd
import numpy as np
from elasticsearch._async.helpers import async_bulk  # noqa
from elasticsearch import AsyncElasticsearch

from mapping import INDEX_MAPPING  # noqa


@lru_cache(maxsize=None)
def boto3_session():
    return aioboto3.Session()


@asynccontextmanager
async def get_s3_client():
    async with boto3_session().client("s3", region_name='us-east-1') as s3:
        yield s3


def process_s3_response_keys(response: Dict[str, str]) -> List[str]:
    s3_lists = []
    for content in response.get('Contents', []):
        s3_lists.append(content['Key'])  # noqa
    return s3_lists


async def get_bunch_of_keys_from_s3(s3_client, bucket_name__, prefix, max_keys=100):
    response = await s3_client.list_objects_v2(
        Bucket=bucket_name__,
        Prefix=prefix,
        MaxKeys=max_keys)
    yield process_s3_response_keys(response)
    continuation_token = response.get('NextContinuationToken')
    while continuation_token:
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name__,
            Prefix=prefix,
            MaxKeys=max_keys,
            ContinuationToken=continuation_token,
        )
        yield process_s3_response_keys(response)
        continuation_token = response.get('NextContinuationToken')


async def get_data_to_insert(s3_key, bucket_name_) -> pd.DataFrame:
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name_, s3_key)
    zip_bytes = obj.get()['Body'].read()
    with gzip.open(BytesIO(zip_bytes), 'r') as f:
        dataframe_current = pd.read_json(f, lines=True)
        keyword_info_df = pd.DataFrame(dataframe_current.keyword_info.values.tolist())
        dataframe_current['cpc'] = keyword_info_df['cpc']
        dataframe_current['competition'] = keyword_info_df['competition']
        dataframe_current['volume'] = keyword_info_df['search_volume']
        dataframe_current['history'] = keyword_info_df['history']
        dataframe_current['categories'] = keyword_info_df['categories']
        dataframe_current[['cpc', 'competition']] = dataframe_current[['cpc', 'competition']].round(4)
        dataframe_current = dataframe_current.replace({np.nan: None})
        return dataframe_current


async def es_create_index_if_does_not_exists(es, index_name):
    await es.indices.create(index=index_name, body=INDEX_MAPPING, ignore=400)


def chunks(terms, chunk_size=5000):
    """
    Yield successive n-sized chunks from lst.
    :param terms: all the terms
    :param chunk_size:
    :return: list of chunk terms
    """
    for ii in range(0, len(terms), chunk_size):
        yield terms[ii:ii + chunk_size]


async def execute_many(chnk, indx, es):
    def doc_generator(chnk_, indx_):
        for document in chnk_:
            try:
                cpc = float(document[9])
            except (ValueError, TypeError):
                cpc = None
            try:
                competition = float(document[10])
            except (ValueError, TypeError):
                competition = None

            doc = {
                "_index": indx_,
                "_source": {
                    "keyword": document[0],
                    "volume": document[11],
                    "cpc": cpc,
                    "competition": competition,
                    "spell_type": document[4],
                    "history": document[12],
                    "categories": document[13],
                },
            }

            yield doc
    try:
        await async_bulk(es, doc_generator(chnk, indx), max_retries=100)
    except Exception as e:
        print(e)
        await execute_many(chnk, indx, es)


async def start_ingestion(df_, indx_name, es):
    tuples = [tuple(x) for x in df_.to_numpy()]
    print(f'Total terms for this file are: {len(tuples)}')
    for chunks_data in chunks(tuples, 15000):
        tasks = []
        for chunk in chunks(chunks_data, 1000):
            tasks.append(execute_many(chunk, indx_name, es))
        await asyncio.gather(*tasks)
    print('File inserted')


def get_parsed_args():
    parser = argparse.ArgumentParser(description='Data delivery system.')
    parser.add_argument('--bucket_name', type=str, default='data-for-seo-adwords-data')
    parser.add_argument('--year', type=str, default='2022')
    parser.add_argument('--month', type=str, default='12')
    parser.add_argument('--es_host', type=str, default='127.0.0.1:9201')
    parser.add_argument('--lower_range', type=int, default=0)
    parser.add_argument('--upper_range', type=int, default=10000)
    return parser.parse_args()


async def process_ingestion(lower_range_, upper_range_):
    async with get_s3_client() as s3_cli:
        async for s3_object_keys in get_bunch_of_keys_from_s3(
            s3_client=s3_cli,
            bucket_name__=bucket_name,
            prefix=f'{year}/{month}/',
            max_keys=300
        ):
            for s3_key in s3_object_keys:
                try:
                    current_file_number = int(s3_key.split('/')[-1].split('.')[0])
                    if current_file_number < lower_range_ or current_file_number >= upper_range_:
                        continue
                except (ValueError, TypeError):
                    continue
                country = s3_key.split('/')[2].lower()
                language = s3_key.split('/')[3].lower()
                es_index_name = f'adwords_{language}_{country}_{year}_{month}'
                await es_create_index_if_does_not_exists(es_client, es_index_name)
                print('s3_key: ', s3_key)
                df = await get_data_to_insert(s3_key, bucket_name)
                await start_ingestion(df, es_index_name, es_client)


if __name__ == '__main__':
    args = get_parsed_args()
    year = args.year
    month = args.month
    bucket_name = args.bucket_name
    es_host = args.es_host
    lower_range = args.lower_range
    upper_range = args.upper_range

    es_client = AsyncElasticsearch(hosts=[es_host], verify_certs=False,
                                   http_auth=('elastic', 'RTfk38CuXJ8EwDNHTMUCqCue2Xa5ePuX'))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_ingestion(lower_range, upper_range))
    loop.run_until_complete(es_client.close())
