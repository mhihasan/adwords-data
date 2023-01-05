import asyncio
from elasticsearch import AsyncElasticsearch


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
    es_client = AsyncElasticsearch(hosts=['localhost:9201'], http_auth=('elastic', 'RTfk38CuXJ8EwDNHTMUCqCue2Xa5ePuX'))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(es_client))
    loop.run_until_complete(es_client.close())
