from abc import ABC, ABCMeta
import asyncio
import logging
import os
import sys
from asyncio.proactor_events import BaseProactorEventLoop

import uvloop

from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, cursor
from psycopg2.extras import DictCursor
from psycaio import connect, AioCursorMixin
from psycaio.cursor_selector import SelectorAioCursorMixin

from psycaio import proactor_connect as connect


async def get_conn():
    cn = await connect(dbname='postgres')
    cr = cn.cursor()
    await cr.execute("SELECT 42")
    print(cr.fetchone()[0])



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.set_debug(False)
    cn = loop.run_until_complete(get_conn())
    del cn
    loop.close()
