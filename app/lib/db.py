import os

import asyncpg
import orjson as json
from dotenv import load_dotenv

load_dotenv()

credentials = {
    "user": os.getenv('POSTGRESQL_USERNAME'),
    "password": os.getenv('POSTGRESQL_PASSWORD'),
    "database": os.getenv('POSTGRESQL_DATABASE'),
    "host": os.getenv('POSTGRESQL_HOST'),
    "port": os.getenv('POSTGRESQL_PORT'),
}


async def set_json_charset(connection: asyncpg.Connection):
    for json_type in ('json', 'jsonb'):
        await connection.set_type_codec(
            json_type,
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )


async def db_connect():
    return await asyncpg.create_pool(init=set_json_charset, **credentials)
