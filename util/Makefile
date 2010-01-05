#
# pg_bulkload.so: lib/Makefile
#
#    Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	pg_timestamp.c
OBJS = $(SRCS:.c=.o)
MODULE_big = pg_timestamp
DATA_built = pg_timestamp.sql
DATA = uninstall_pg_timestamp.sql
PG_CPPFLAGS = -I../include

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bulkload
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
