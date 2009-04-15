#
# pg_bulkload: Makefile
#
#    Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
.PHONY: all install clean

all: 
	make -C bin
	make -C lib
	make -C util

install: 
	make -C bin install
	make -C lib install
	make -C util install

clean:
	make -C bin clean
	make -C lib clean
	make -C util clean
