import asyncio

from elasticsearch import AsyncElasticsearch

es_client = AsyncElasticsearch(hosts=['http://localhost:9201'], http_auth=('elastic', 'RTfk38CuXJ8EwDNHTMUCqCue2Xa5ePuX'))


async def main(es):
    resp = await es.search(
        index="adwords_ar_ae_2022_12",
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
                                    "query": "manage emirates",
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
    print(resp)

if __name__ == '__main__':
    asyncio.run(main(es_client))