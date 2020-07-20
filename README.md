# psycaio

A Python asyncio wrapper around psycopg2

## Example

```Python
import asyncio

from psycaio import connect


async def test_conn():
    cn = await connect(dbname='postgres')
    cr = cn.cursor()
    await cr.execute("SELECT 42")
    print(cr.fetchone()[0])
    await cn.commit()
    cr = cn.cursor()
    await cr.execute("SELECT 42")
    print(cr.fetchone()[0])
    await cn.rollback()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_conn())
    loop.close()

```


## About

This package is meant as a minimal asyncio mixin for psycopg2.
