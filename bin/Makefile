#
# pg_bulkload: bin/Makefile
#
#    Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	pg_bulkload.c \
	pg_crc.c
OBJS = $(SRCS:.c=.o)
PROGRAM = pg_bulkload
SCRIPTS = postgresql
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

pg_crc.c: $(top_srcdir)/src/backend/utils/hash/pg_crc.c
	rm -f $@ && $(LN_S) $< .

.PHONY: subclean
clean: subclean

subclean:
	rm -f pg_crc.c
