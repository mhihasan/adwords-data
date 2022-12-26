import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

import asyncpg

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)


db_params = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


@asynccontextmanager
async def db_connection(**kwargs):
    conn = await asyncpg.connect(
        user=kwargs.get("user"),
        password=kwargs.get("password"),
        database=kwargs.get("database"),
        host=kwargs.get("host"),
        port=kwargs.get("port"),
    )
    yield conn
    await conn.close()


async def search_adwords_keywords(term, search_type="phrase", total_keywords=100):
    separator = {"phrase": "<->", "broad": "&"}[search_type]
    q = f"{separator}".join(term.split(" "))

    search_query = f"""
        with searched_keywrods as (
            select *
            from adwords_en_us, to_tsquery('{q}') as query
            where keyword_tsv @@ query
        )
        select keyword, volume
        from searched_keywrods
        where spell_type is null and volume is not null
        order by volume desc
        limit {total_keywords};
    """

    async with db_connection(**db_params) as conn:
        values = await conn.fetch(search_query)

    return [
        {
            "keyword": value["keyword"],
            "volume": value["volume"],
        }
        for value in values
    ]


async def main():
    multi_word_term = 'law apartment'
    for search_type in ["phrase", "broad"]:
        print(f"<<<<<<<<< Search type: {search_type}, term: {multi_word_term} >>>>>>>>>")
        result = await search_adwords_keywords(multi_word_term, search_type=search_type)
        for r in result:
            print(r)
        print("\n")

    singe_word_term = 'apartment'
    for search_type in ['broad']:
        print(f"<<<<<<<<< Search type: {search_type}, term: {singe_word_term} >>>>>>>>>")
        result = await search_adwords_keywords(singe_word_term, search_type=search_type)
        for r in result:
            print(r)
        print("\n")


if __name__ == "__main__":
   asyncio.run(main())
