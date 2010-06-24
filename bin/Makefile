#
# pg_bulkload: bin/Makefile
#
#    Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = pg_bulkload.c recovery.c pgut/pgut.c pgut/pgut-fe.c pgut/pgut-list.c
OBJS = $(SRCS:.c=.o)
PROGRAM = pg_bulkload
SCRIPTS = postgresql
REGRESS = init load_bin load_csv load_remote load_function load_encoding load_check load_filter
#REGRESS = load_parallel

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

# remove dependency to libxml2 and libxslt
LIBS := $(filter-out -lxml2, $(LIBS))
LIBS := $(filter-out -lxslt, $(LIBS))

ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
sql/load_function.sql: sql/load_function-$(MAJORVERSION).sql
	cp sql/load_function-$(MAJORVERSION).sql sql/load_function.sql
sql/load_filter.sql: sql/load_filter-$(MAJORVERSION).sql
	cp sql/load_filter-$(MAJORVERSION).sql sql/load_filter.sql
expected/load_function.out: expected/load_function-$(MAJORVERSION).out
	cp expected/load_function-$(MAJORVERSION).out expected/load_function.out
expected/load_filter.out: expected/load_filter-$(MAJORVERSION).out
	cp expected/load_filter-$(MAJORVERSION).out expected/load_filter.out

.PHONY: subclean
clean: subclean

subclean:
	rm -f sql/load_function.sql sql/load_filter.sql expected/load_function.out expected/load_filter.out

installcheck: sql/load_function.sql sql/load_filter.sql expected/load_function.out expected/load_filter.out
