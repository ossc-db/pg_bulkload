/*
 * pg_bulkload: include/binary.h
 *
 *	  Copyright (c) 2011-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of binary format
 *
 */
#ifndef BINARY_H_INCLUDED
#define BINARY_H_INCLUDED

#include "reader.h"

typedef struct Field	Field;
typedef Datum (*Read)(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
typedef void (*Write)(char *out, size_t len, Datum value, bool null);

struct Field
{
	Read	read;		/**< parse function of the field */
	Write	write;		/**< write function of the field */
	int		offset;		/**< offset from head */
	int		len;		/**< byte length of the field */
	char   *nullif;		/**< null pattern, if any */
	int		nulllen;	/**< length of nullif */
	char   *in;			/**< pointer to the character string or binary */
	bool	character;	/**< field is CHAR or VARCHAR? */
	Oid		typeid;		/**< field typeid */
	char   *str;		/**< work buffer */
};

extern void BinaryParam(Field **fields, int *nfield, char *value, bool preserve_blanks, bool length_only);
extern int BinaryDumpParam(Field *field, StringInfo buf, int offset);
extern void BinaryDumpParams(Field *fields, int nfield, StringInfo buf, char *param);

#endif   /* BINARY_H_INCLUDED */
