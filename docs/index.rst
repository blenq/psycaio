.. psycaio documentation master file

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   quickstart
   usage/installation
   module

psycaio
=======

Psycaio is an AsyncIO mixin library for psycopg2.

It uses the mechanism provided by psycopg2 to instantiate custom classes for
both connection and cursor objects. These custom classes override the blocking
calls and change those into coroutines.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
