Psycaio Module
==============

.. py:module:: psycaio

.. autofunction:: connect

.. autoclass:: AioConnMixin
   :members: cursor, get_notify, get_notify_nowait, close, cancel

.. autoclass:: AioConnection
   :show-inheritance:

.. autoclass:: AioCursorMixin
   :members: execute, callproc, executemany

.. autoclass:: AioCursor
   :show-inheritance:

.. _psycopg2 connect function: https://www.psycopg.org/docs/module.html#psycopg2.connect
.. _psycopg2 connection: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.connection
.. _psycopg2 cursor: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.cursor
