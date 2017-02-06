/*
 * pg_bulkload: util/pg_timestamp.c
 *
 *	  Copyright (c) 2007-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#include "postgres.h"

#include "utils/datetime.h"

#if PG_VERSION_NUM >= 100000
#include "utils/builtins.h"
#endif

PG_MODULE_MAGIC;

/* prototype of static function */
static void AdjustTimestampForTypmod(Timestamp *time, int32 typmod);
extern Datum pg_timestamp_in(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_timestamp_in);

/**
 * @brief Convert the given character string into an internal representation
 * of the timestamp type.
 *
 * Flow:
 * -# Verify if each of the four digit representing "year" is a number from zero
 *	  to nine respectively.
 * -# Verify if the following chareacter between "year" and "month" is '-'.
 * -# Verify if each of the following two characters represeinting "month" is
 *	  a number from zero to nine respectively.
 * -# Verify if the following character between "month" and "day" is '-'.
 * -# Verify if each of the following two characters representing "day" is
 *	  a number from zero to nine respectively.
 * -# Verify if the following character betwen "day" and "hour" is ' '
 *	  (ASCII SPACE).
 * -# Verify if each of the following two characres representing "hour" is
 *	  a number from zero to nine respectively.
 * -# Verify if the folliwing character between "hour" and "minute" is '-'.
 * -# Verify if each of the following two characters representing "minute"
 *	  is a number from zero to nine respectively.
 * -# Verify if the following character between "minute" and "second" is '-'.
 * -# Verify if each of the following two characters representing "second"
 *	  is a number from zero to nine respectively.
 * -# Verify if the following character is '\0'.
 * -# Store year, month, day, hour, minute and second value to corresponding
 *	  ps_tm structure members.
 * -# Convert to PostgreSQL internal format.
 *
 * @param PG_FUNCTION_ARGS
 *		   Fist argument: Character string representing a date
 *		   Second argument: OID(This argument is passed to original
 *							timestamp_in() of PostgreSQL)
 *		   Third argument: typmod(This argument is passed to
 *						   AdjustTimestampForTypmod())
 *
 * @return Date value converted into PostgreSQL internal representation.
 *
 * For quick parse, the format of an input is limited to the following.
 *			 YYYY-MM-DD hh:mm:ss
 *	Example) 2006-02-12 15:30:12
 * - Input character length must be 19 bytes.
 * - Only '0' (0x30) to '9' (0x39) is allowed to represent year, month, day,
 *	 hour, minute and second.
 * - Delimiter between year and month, and month and day, must be single '-' (0x2D).
 * - There must be single ASCII space (0x20) between date and hour.
 * - Single ':' (0x3A) must be used as a delimiter between hour and minute, and
 *	 munte and second.
 *
 * @note If input character string does not follow the format here, input
 *		 string will be passed to PostgreSQL original timestamp_in().
 */
Datum
pg_timestamp_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Timestamp	result;
	struct pg_tm tt,
			   *tm = &tt;

	/**
	 *	Get date and time from the input character string, convert to
	 *	numbers and then store them to pg_tm structure.
	 *	Returns when date and time are handled.
	 */
	/*
	 * Validate the year.
	 */
	if (!isdigit(str[0]) || !isdigit(str[1]) ||
		!isdigit(str[2]) || !isdigit(str[3]))
		return timestamp_in(fcinfo);

	/*
	 * Validate the delimiter.
	 */
	if (str[4] != '-')
		return timestamp_in(fcinfo);

	/*
	 * Validate the month.
	 */
	if (!isdigit(str[5]) || !isdigit(str[6]))
		return timestamp_in(fcinfo);

	/*
	 * Validate the delimiter.
	 */
	if (str[7] != '-')
		return timestamp_in(fcinfo);

	/*
	 * Validate the date.
	 */
	if (!isdigit(str[8]) || !isdigit(str[9]))
		return timestamp_in(fcinfo);

	/*
	 * Validte the space.
	 */
	if (str[10] != ' ')
		return timestamp_in(fcinfo);

	/*
	 * Validate the hour.
	 */
	if (!isdigit(str[11]) || !isdigit(str[12]))
		return timestamp_in(fcinfo);

	/*
	 * Validate the delimiter.
	 */
	if (str[13] != ':')
		return timestamp_in(fcinfo);

	/*
	 * Validate the minute.
	 */
	if (!isdigit(str[14]) || !isdigit(str[15]))
		return timestamp_in(fcinfo);

	/*
	 * Validate the delimiter.
	 */
	if (str[16] != ':')
		return timestamp_in(fcinfo);

	/*
	 * Validate the second.
	 */
	if (!isdigit(str[17]) || !isdigit(str[18]))
		return timestamp_in(fcinfo);

	/*
	 * Validate the terminator
	 */
	if (str[19] != '\0')
		return timestamp_in(fcinfo);

	/*
	 * Convert the year and store.
	 */
	tm->tm_year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 +
		(str[2] - '0') * 10 + (str[3] - '0');

	/*
	 * Convert the month and store.
	 */
	tm->tm_mon = (str[5] - '0') * 10 + (str[6] - '0');

	/*
	 * Convert the date and store.
	 */
	tm->tm_mday = (str[8] - '0') * 10 + (str[9] - '0');

	/*
	 * Convert the hour and store.
	 */
	tm->tm_hour = (str[11] - '0') * 10 + (str[12] - '0');

	/*
	 * Convert the minute and store.
	 */
	tm->tm_min = (str[14] - '0') * 10 + (str[15] - '0');

	/*
	 * Convert the second and store.
	 */
	tm->tm_sec = (str[17] - '0') * 10 + (str[18] - '0');

	/*
	 * Convert the date and time into PostgreSQL internal representation.
	 */
	if (tm2timestamp(tm, 0, NULL, &result) != 0)
		ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						errmsg("timestamp out of range: \"%s\"", str)));

	/*
	 * Adjust the value
	 */
	AdjustTimestampForTypmod(&result, typmod);

	/*
	 * Return the result value
	 */
	PG_RETURN_TIMESTAMP(result);
}

/*------------------------------------------------------------------------
 *	 The following lines were taken from the original PostgreSQL source.
 *	   - AdjustTimestampForTypmod		No modification is made here.
 *		  + backend/utils/adt/timestamp.c:4277
 *------------------------------------------------------------------------*/

/**
 * @brief Adjust the timestamp value.
 *
 */
static void
AdjustTimestampForTypmod(Timestamp *time, int32 typmod)
{
#ifdef HAVE_INT64_TIMESTAMP
	static const int64 TimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimestampOffsets[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};
#else
	static const double TimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		1,
		10,
		100,
		1000,
		10000,
		100000,
		1000000
	};
#endif

	if (!TIMESTAMP_NOT_FINITE(*time)
		&& (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION))
	{
		if (typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg
					 ("timestamp(%d) precision must be between %d and %d",
					  typmod, 0, MAX_TIMESTAMP_PRECISION)));

		/*
		 * Note: this round-to-nearest code is not completely consistent about
		 * rounding values that are exactly halfway between integral values.
		 * On most platforms, rint() will implement round-to-nearest-even, but
		 * the integer code always rounds up (away from zero).	Is it worth
		 * trying to be consistent?
		 */
#ifdef HAVE_INT64_TIMESTAMP
		if (*time >= INT64CONST(0))
		{
			*time =
				((*time +
				  TimestampOffsets[typmod]) / TimestampScales[typmod]) *
				TimestampScales[typmod];
		}
		else
		{
			*time =
				-((((-*time) +
					TimestampOffsets[typmod]) / TimestampScales[typmod]) *
				  TimestampScales[typmod]);
		}
#else
		*time =
			rint((double) *time * TimestampScales[typmod]) /
			TimestampScales[typmod];
#endif
	}
}
