#
# pg_bulkload: bin/Makefile
#
#    Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = pg_bulkload.c recovery.c pgut/pgut.c pgut/pgut-list.c
OBJS = $(SRCS:.c=.o)
PROGRAM = pg_bulkload
SCRIPTS = postgresql
REGRESS = init load_bin load_csv load_remote load_parallel load_function

PG_CPPFLAGS = -I../include -I$(libpq_srcdir)
PG_LIBS = $(libpq)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bulkload
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
