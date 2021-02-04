#
# pg_bulkload: lib/Makefile
#
#    Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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

#supports both EXTENSION (for >=9.1) and without_EXTENSION (for <PG 9.1)
DATA_built = pg_bulkload.sql
DATA = pg_bulkload--1.0.sql pg_bulkload--unpackaged--1.0.sql uninstall_pg_bulkload.sql 

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
