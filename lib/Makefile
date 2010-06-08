#
# pg_bulkload: lib/Makefile
#
#    Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
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
	writer_buffered.c \
	writer_direct.c \
	writer_parallel.c \
	pgut/pgut-be.c \
	pgut/pgut-ipc.c
OBJS = $(SRCS:.c=.o)
MODULE_big = pg_bulkload
DATA_built = pg_bulkload.sql
DATA = uninstall_pg_bulkload.sql 
PG_CPPFLAGS = -I../include -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

ifdef USE_PROFILE
PG_CPPFLAGS += -DENABLE_BULKLOAD_PROFILE
endif

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
else
subdir = contrib/pg_bulkload
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# remove dependency to libxml2 and libxslt
LIBS := $(filter-out -lxml2, $(LIBS))
LIBS := $(filter-out -lxslt, $(LIBS))

.PHONY: subclean
clean: subclean

subclean:
	rm -f nbtsort.c
