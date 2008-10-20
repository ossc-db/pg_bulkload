#
# pg_bulkload: lib/Makefile
#
#    Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	pg_btree.c \
	pg_bulkload.c \
	pg_controlinfo.c \
	pg_heap_buffered.c \
	pg_heap_direct.c \
	pg_input_csv.c \
	pg_input_fixed.c \
	pg_strutil.c \
	trigger.c
OBJS = $(SRCS:.c=.o)
MODULE_big = pg_bulkload
DATA_built = pg_bulkload.sql
DATA = uninstall_pg_bulkload.sql 
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
