pg_bulkload
=======
pg_bulkload is a high speed data loading tool for PostgreSQL.

pg_bulkload is designed to load huge amount of data to a database. 
You can load data to table bypassing PostgreSQL shared buffers.

pg_bulkload also has some ETL features; input data validation and data transformation.

Branches
--------

* master : branch for pg_bulkload 3.2
* VERSION3_1_STABLE : branch for pg_bulkload 3.1 which supports PostgreSQL 8.4 to 9.4. 

How to use
----------
See doc/index.html.

How to build and install from source code
-----------------------------------------
Change directory into top directory of pg_bulkload sorce codes and
run the below commands.

````
 $ make
 # make install
````

How to run regression tests
---------------------------
Start PostgreSQL server and run the below command.

````
 $ make installcheck
````

Bug report
----------

https://sourceforge.net/p/pgbulkload/tickets/




