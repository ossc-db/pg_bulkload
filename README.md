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
pg_bulkload works with control file like below,

````
$ pg_bulkload sample_csv.ctl
NOTICE: BULK LOAD START
NOTICE: BULK LOAD END
	0 Rows skipped.
	8 Rows successfully loaded.
	0 Rows not loaded due to parse errors.
	0 Rows not loaded due to duplicate errors.
	0 Rows replaced with new rows.
````

See documentation about detail usage.

http://ossc-db.github.io/pg_bulkload/index.html

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




