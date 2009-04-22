#
# pg_bulkload: bin/Makefile
#
#    Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = pg_bulkload.c pgut/pgut.c
OBJS = $(SRCS:.c=.o)
PROGRAM = pg_bulkload
SCRIPTS = postgresql
REGRESS = init load_bin load_csv

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
