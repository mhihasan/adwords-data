import asyncio

from fts_elastic.es_client import get_es_client

INDEX_MAPPING = {
    "settings": {
        "number_of_replicas": 1,
        "number_of_shards": 10,
        "index": {"sort.field": "volume", "sort.order": "desc"},
    },
    "mappings": {
        "dynamic": False,
        "properties": {
            "keyword": {"type": "text"},
            "volume": {"type": "long"},
        },
    },
}


async def create_index(es_client, index_name):
    """Creates an index in Elasticsearch if one isn't already there."""
    await es_client.indices.create(
        index=index_name,
        mappings={
            "dynamic": False,
            "properties": {
                "keyword": {"type": "text", "analyzer": "english"},
                "volume": {"type": "long"},
            },
        },
        settings={
            "number_of_replicas": 1,
            "number_of_shards": 10,
            "index": {"sort.field": "volume", "sort.order": "desc"},
        },
        # ignore=400,
    )


async def main():
    async with get_es_client() as es_client:
        await create_index(es_client, 'adwords_en_us_2022_12')
        # r = await es_client.indices.delete(index='adwords_en_us_2022_12')
        # print(r)


if __name__ == "__main__":
    asyncio.run(main())
