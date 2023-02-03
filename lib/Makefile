#
# pg_bulkload: lib/Makefile
#
#    Copyright (c) 2007-2023, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	binary.c \
	logger.c \
	parser_binary.c \
	parser_csv.c \
	parser_function.c \
	parser_tuple.c \
	pg_btree.c \
	pg_bulkload.c \
	pg_strutil.c \
	reader.c \
	source.c \
	writer.c \
	writer_binary.c \
	writer_buffered.c \
	writer_direct.c \
	writer_parallel.c \
	pgut/pgut-be.c \
	pgut/pgut-ipc.c
OBJS = $(SRCS:.c=.o)
MODULE_big = pg_bulkload

EXTENSION = pg_bulkload

# set pg_bulkload version
BULKLOAD_VERSION = 3.1.20

#supports both EXTENSION (for >=9.1) and without_EXTENSION (for <PG 9.1)
DATA_built = pg_bulkload.sql \
	pg_bulkload--$(BULKLOAD_VERSION).sql \
	pg_bulkload--unpackaged--$(BULKLOAD_VERSION).sql \
	uninstall_pg_bulkload.sql \
	pg_bulkload.control


PG_CPPFLAGS = -I../include -I$(libpq_srcdir)
SHLIB_LINK = $(libpq) -lpthread

# handle to which function to be published from this shared library
# see src/Makefile.shlib in PostgreSQL
SHLIB_EXPORTS = exports.txt

ifdef USE_PROFILE
PG_CPPFLAGS += -DENABLE_BULKLOAD_PROFILE
endif

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

.PHONY: subclean
clean: subclean

subclean:
	rm -f nbtsort.c

LDFLAGS+=-Wl,--build-id

pg_bulkload--$(BULKLOAD_VERSION).sql: pg_bulkload.sql.in
	sed 's,BULKLOAD_VERSION,$(BULKLOAD_VERSION),g' $< > $@

pg_bulkload.sql: pg_bulkload--$(BULKLOAD_VERSION).sql
	sed 's,BULKLOAD_VERSION,$(BULKLOAD_VERSION),g' $< > $@

pg_bulkload--unpackaged--$(BULKLOAD_VERSION).sql: pg_bulkload--unpackaged.sql.in
	sed 's,BULKLOAD_VERSION,$(BULKLOAD_VERSION),g' $< > $@

uninstall_pg_bulkload.sql: uninstall_pg_bulkload.sql.in

pg_bulkload.control: pg_bulkload.control.in
	sed 's,BULKLOAD_VERSION,$(BULKLOAD_VERSION),g' $< > $@