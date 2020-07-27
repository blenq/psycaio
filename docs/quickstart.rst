Quickstart
==========


Using psycaio is very similar to developing with psycopg2. The only difference
is that a few blocking methods are replaced with asyncio coroutines. 
 
.. code-block:: python

    import asyncio
    
    from psycaio import connect
    
    
    async def test_conn():
        cn = await connect(dbname='postgres')
        cr = cn.cursor()
        await cr.execute("SELECT %s", (42,))
        print(cr.fetchone()[0])
    
    
    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(test_conn())
        loop.close()