#
# pg_bulkload: bin/Makefile
#
#    Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = pg_bulkload.c recovery.c pgut/pgut.c pgut/pgut-fe.c pgut/pgut-list.c
OBJS = $(SRCS:.c=.o)
PROGRAM = pg_bulkload
SCRIPTS = postgresql
REGRESS = init load_bin load_csv load_remote load_function load_encoding load_check load_filter load_parallel write_bin

PG_CPPFLAGS = -I../include -I$(libpq_srcdir)
PG_LIBS = $(libpq)

ifndef USE_PGXS
top_builddir = ../../..
makefile_global = $(top_builddir)/src/Makefile.global
ifeq "$(wildcard $(makefile_global))" ""
USE_PGXS = 1	# use pgxs if not in contrib directory
endif
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/$(MODULE_big)
include $(makefile_global)
include $(top_srcdir)/contrib/contrib-global.mk
endif

# remove dependency to libxml2 and libxslt
LIBS := $(filter-out -lxml2, $(LIBS))
LIBS := $(filter-out -lxslt, $(LIBS))

ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

REGRESS_OPTS += $(if $(filter 8.3 8.4 9.0, $(MAJORVERSION)), --multibyte=UTF8, --encoding=UTF8)

sql/init.sql: sql/init-$(MAJORVERSION).sql
	cp sql/init-$(MAJORVERSION).sql sql/init.sql
sql/init-9.3.sql:
	cp sql/init-extension.sql sql/init-9.3.sql
sql/init-9.4.sql:
	cp sql/init-extension.sql sql/init-9.4.sql
sql/init-9.5.sql:
	cp sql/init-extension.sql sql/init-9.5.sql
sql/init-9.6.sql:
	cp sql/init-extension.sql sql/init-9.6.sql
sql/init-10.sql:
	cp sql/init-extension.sql sql/init-10.sql
sql/init-11.sql:
	cp sql/init-extension.sql sql/init-11.sql
sql/init-12.sql:
	cp sql/init-extension-v2.sql sql/init-12.sql

sql/load_bin.sql: sql/load_bin-$(MAJORVERSION).sql
	cp sql/load_bin-$(MAJORVERSION).sql sql/load_bin.sql
sql/load_bin-9.3.sql:
	cp sql/load_bin-v1.sql sql/load_bin-9.3.sql
sql/load_bin-9.4.sql:
	cp sql/load_bin-v1.sql sql/load_bin-9.4.sql
sql/load_bin-9.5.sql:
	cp sql/load_bin-v1.sql sql/load_bin-9.5.sql
sql/load_bin-9.6.sql:
	cp sql/load_bin-v1.sql sql/load_bin-9.6.sql
sql/load_bin-10.sql:
	cp sql/load_bin-v1.sql sql/load_bin-10.sql
sql/load_bin-11.sql:
	cp sql/load_bin-v1.sql sql/load_bin-11.sql
sql/load_bin-12.sql:
	cp sql/load_bin-v2.sql sql/load_bin-12.sql

sql/load_csv.sql: sql/load_csv-$(MAJORVERSION).sql
	cp sql/load_csv-$(MAJORVERSION).sql sql/load_csv.sql
sql/load_csv-9.3.sql:
	cp sql/load_csv-v1.sql sql/load_csv-9.3.sql
sql/load_csv-9.4.sql:
	cp sql/load_csv-v1.sql sql/load_csv-9.4.sql
sql/load_csv-9.5.sql:
	cp sql/load_csv-v1.sql sql/load_csv-9.5.sql
sql/load_csv-9.6.sql:
	cp sql/load_csv-v1.sql sql/load_csv-9.6.sql
sql/load_csv-10.sql:
	cp sql/load_csv-v1.sql sql/load_csv-10.sql
sql/load_csv-11.sql:
	cp sql/load_csv-v1.sql sql/load_csv-11.sql
sql/load_csv-12.sql:
	cp sql/load_csv-v2.sql sql/load_csv-12.sql

sql/load_filter.sql: sql/load_filter-$(MAJORVERSION).sql
	cp sql/load_filter-$(MAJORVERSION).sql sql/load_filter.sql
sql/load_filter-9.3.sql:
	cp sql/load_filter-v2.sql sql/load_filter-9.3.sql
sql/load_filter-9.4.sql:
	cp sql/load_filter-v2.sql sql/load_filter-9.4.sql
sql/load_filter-9.5.sql:
	cp sql/load_filter-v2.sql sql/load_filter-9.5.sql
sql/load_filter-9.6.sql:
	cp sql/load_filter-v2.sql sql/load_filter-9.6.sql
sql/load_filter-10.sql:
	cp sql/load_filter-v2.sql sql/load_filter-10.sql
sql/load_filter-11.sql:
	cp sql/load_filter-v2.sql sql/load_filter-11.sql
sql/load_filter-12.sql:
	cp sql/load_filter-v3.sql sql/load_filter-12.sql

sql/load_function.sql: sql/load_function-$(MAJORVERSION).sql
	cp sql/load_function-$(MAJORVERSION).sql sql/load_function.sql
sql/load_function-9.3.sql:
	cp sql/load_function-v2.sql sql/load_function-9.3.sql
sql/load_function-9.4.sql:
	cp sql/load_function-v2.sql sql/load_function-9.4.sql
sql/load_function-9.5.sql:
	cp sql/load_function-v2.sql sql/load_function-9.5.sql
sql/load_function-9.6.sql:
	cp sql/load_function-v2.sql sql/load_function-9.6.sql
sql/load_function-10.sql:
	cp sql/load_function-v2.sql sql/load_function-10.sql
sql/load_function-11.sql:
	cp sql/load_function-v2.sql sql/load_function-11.sql
sql/load_function-12.sql:
	cp sql/load_function-v3.sql sql/load_function-12.sql

.PHONY: subclean
clean: subclean

subclean:
	rm -f sql/init.sql sql/init-{9.3,9.4,9.5,9.6,10,11,12}.sql
	rm -f sql/load_bin.sql sql/load_bin-{9.3,9.4,9.5,9.6,10,11,12}.sql
	rm -f sql/load_csv.sql sql/load_csv-{9.3,9.4,9.5,9.6,10,11,12}.sql
	rm -f sql/load_filter.sql sql/load_filter-{9.3,9.4,9.5,9.6,10,11,12}.sql
	rm -f sql/load_function.sql sql/load_function-{9.3,9.4,9.5,9.6,10,11,12}.sql

installcheck: sql/init.sql sql/load_bin.sql sql/load_csv.sql sql/load_function.sql sql/load_filter.sql

LDFLAGS+=-Wl,--build-id
