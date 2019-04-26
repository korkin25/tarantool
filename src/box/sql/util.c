/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Utility functions used throughout sql.
 *
 * This file contains functions for allocating memory, comparing
 * strings, and stuff like that.
 *
 */
#include "sqlInt.h"
#include <stdarg.h>
#if HAVE_ISNAN || SQL_HAVE_ISNAN
#include <math.h>
#endif
#include "coll/coll.h"
#include <unicode/ucasemap.h>
#include "errinj.h"

/*
 * Routine needed to support the testcase() macro.
 */
#ifdef SQL_COVERAGE_TEST
void
sqlCoverage(int x)
{
	static unsigned dummy = 0;
	dummy += (unsigned)x;
}
#endif

/*
 * Give a callback to the test harness that can be used to simulate faults
 * in places where it is difficult or expensive to do so purely by means
 * of inputs.
 *
 * The intent of the integer argument is to let the fault simulator know
 * which of multiple sqlFaultSim() calls has been hit.
 *
 * Return whatever integer value the test callback returns, or return
 * SQL_OK if no test callback is installed.
 */
#ifndef SQL_UNTESTABLE
int
   sqlFaultSim(int iTest)
{
	int (*xCallback) (int) = sqlGlobalConfig.xTestCallback;
	return xCallback ? xCallback(iTest) : SQL_OK;
}
#endif

/*
 * Return true if the floating point value is Not a Number (NaN).
 *
 * Use the math library isnan() function if compiled with SQL_HAVE_ISNAN.
 * Otherwise, we have our own implementation that works on most systems.
 */
int
sqlIsNaN(double x)
{
	int rc;			/* The value return */
#if !SQL_HAVE_ISNAN && !HAVE_ISNAN
	/*
	 * Systems that support the isnan() library function should probably
	 * make use
	 *
	 * This NaN test sometimes fails if compiled on GCC with -ffast-math.
	 * On the other hand, the use of -ffast-math comes with the following
	 * warning:
	 *
	 *      This option [-ffast-math] should never be turned on by any
	 *      -O option since it can result in incorrect output for programs
	 *      which depend on an exact implementation of IEEE or ISO
	 *      rules/specifications for math functions.
	 */
#ifdef __FAST_MATH__
#error sql will not work correctly with the -ffast-math option of GCC.
#endif
	volatile double y = x;
	volatile double z = y;
	rc = (y != z);
#else				/* if HAVE_ISNAN */
	rc = isnan(x);
#endif				/* HAVE_ISNAN */
	testcase(rc);
	return rc;
}

/*
 * Compute a string length that is limited to what can be stored in
 * lower 30 bits of a 32-bit unsigned integer.
 *
 * The value returned will never be negative.  Nor will it ever be greater
 * than the actual length of the string.  For very long strings (greater
 * than 1GiB) the value returned might be less than the true string length.
 */
unsigned
sqlStrlen30(const char *z)
{
	if (z == 0)
		return 0;
	return 0x3fffffff & (unsigned)strlen(z);
}

/*
 * Helper function for sqlError() - called rarely.  Broken out into
 * a separate routine to avoid unnecessary register saves on entry to
 * sqlError().
 */
static SQL_NOINLINE void
sqlErrorFinish(sql * db, int err_code)
{
	sqlSystemError(db, err_code);
}

/*
 * Set the current error code to err_code and clear any prior error message.
 * Also set iSysErrno (by calling sqlSystem) if the err_code indicates
 * that would be appropriate.
 */
void
sqlError(sql * db, int err_code)
{
	assert(db != 0);
	if (err_code)
		sqlErrorFinish(db, err_code);
}

/*
 * Load the sql.iSysErrno field if that is an appropriate thing
 * to do based on the sql error code in rc.
 */
void
sqlSystemError(sql * db, int rc)
{
	if (rc == SQL_IOERR_NOMEM)
		return;
	rc &= 0xff;
	if (rc == SQL_CANTOPEN || rc == SQL_IOERR) {
		db->iSysErrno = sqlOsGetLastError(db->pVfs);
	}
}

/*
 * Set the most recent error code and error string for the sql
 * handle "db". The error code is set to "err_code".
 *
 * If it is not NULL, string zFormat specifies the format of the
 * error string in the style of the printf functions: The following
 * format characters are allowed:
 *
 *      %s      Insert a string
 *      %z      A string that should be freed after use
 *      %d      Insert an integer
 *      %T      Insert a token
 *      %S      Insert the first element of a SrcList
 *
 * zFormat and any string tokens that follow it are assumed to be
 * encoded in UTF-8.
 *
 * To clear the most recent error for sql handle "db", sqlError
 * should be called with err_code set to SQL_OK and zFormat set
 * to NULL.
 */
void
sqlErrorWithMsg(sql * db, int err_code, const char *zFormat, ...)
{
	assert(db != 0);
	sqlSystemError(db, err_code);
	if (zFormat == 0)
		sqlError(db, err_code);
}

/*
 * Convert an SQL-style quoted string into a normal string by removing
 * the quote characters.  The conversion is done in-place.  If the
 * input does not begin with a quote character, then this routine
 * is a no-op.
 *
 * The input string must be zero-terminated.  A new zero-terminator
 * is added to the dequoted string.
 *
 * The return value is -1 if no dequoting occurs or the length of the
 * dequoted string, exclusive of the zero terminator, if dequoting does
 * occur.
 *
 * 2002-Feb-14: This routine is extended to remove MS-Access style
 * brackets from around identifiers.  For example:  "[a-b-c]" becomes
 * "a-b-c".
 */
void
sqlDequote(char *z)
{
	char quote;
	int i, j;
	if (z == 0)
		return;
	quote = z[0];
	if (!sqlIsquote(quote))
		return;
	for (i = 1, j = 0;; i++) {
		assert(z[i]);
		if (z[i] == quote) {
			if (z[i + 1] == quote) {
				z[j++] = quote;
				i++;
			} else {
				break;
			}
		} else {
			z[j++] = z[i];
		}
	}
	z[j] = 0;
}

int
sql_normalize_name(char *dst, int dst_size, const char *src, int src_len)
{
	assert(src != NULL);
	assert(dst != NULL && dst_size > 0);
	if (sqlIsquote(src[0])){
		memcpy(dst, src, src_len);
		dst[src_len] = '\0';
		sqlDequote(dst);
		return src_len + 1;
	}
	UErrorCode status = U_ZERO_ERROR;
	assert(icu_ucase_default_map != NULL);
	int len = ucasemap_utf8ToUpper(icu_ucase_default_map, dst, dst_size,
				       src, src_len, &status);
	assert(U_SUCCESS(status) || status == U_BUFFER_OVERFLOW_ERROR);
	return len + 1;
}

char *
sql_normalized_name_db_new(struct sql *db, const char *name, int len)
{
	int size = len + 1;
	ERROR_INJECT(ERRINJ_SQL_NAME_NORMALIZATION, {
		diag_set(OutOfMemory, size, "sqlDbMallocRawNN", "res");
		return NULL;
	});
	char *res = sqlDbMallocRawNN(db, size);
	if (res == NULL)
		return NULL;
	int rc = sql_normalize_name(res, size, name, len);
	if (rc <= size)
		return res;

	size = rc;
	res = sqlDbReallocOrFree(db, res, size);
	if (res == NULL)
		return NULL;
	if (sql_normalize_name(res, size, name, len) > size)
		unreachable();
	return res;
}

char *
sql_normalized_name_region_new(struct region *r, const char *name, int len)
{
	int size = len + 1;
	ERROR_INJECT(ERRINJ_SQL_NAME_NORMALIZATION, {
		diag_set(OutOfMemory, size, "region_alloc", "res");
		return NULL;
	});
	size_t region_svp = region_used(r);
	char *res = region_alloc(r, size);
	if (res == NULL)
		goto oom_error;
	int rc = sql_normalize_name(res, size, name, len);
	if (rc <= size)
		return res;

	size = rc;
	region_truncate(r, region_svp);
	res = region_alloc(r, size);
	if (res == NULL)
		goto oom_error;
	if (sql_normalize_name(res, size, name, len) > size)
		unreachable();
	return res;

oom_error:
	diag_set(OutOfMemory, size, "region_alloc", "res");
	return NULL;
}

/* Convenient short-hand */
#define UpperToLower sqlUpperToLower

/*
 * Some systems have stricmp().  Others have strcasecmp().  Because
 * there is no consistency, we will define our own.
 *
 * IMPLEMENTATION-OF: R-30243-02494 The sql_stricmp() and
 * sql_strnicmp() APIs allow applications and extensions to compare
 * the contents of two buffers containing UTF-8 strings in a
 * case-independent fashion, using the same definition of "case
 * independence" that sql uses internally when comparing identifiers.
 */
int
sql_stricmp(const char *zLeft, const char *zRight)
{
	if (zLeft == 0) {
		return zRight ? -1 : 0;
	} else if (zRight == 0) {
		return 1;
	}
	return sqlStrICmp(zLeft, zRight);
}

int
sqlStrICmp(const char *zLeft, const char *zRight)
{
	unsigned char *a, *b;
	int c;
	a = (unsigned char *)zLeft;
	b = (unsigned char *)zRight;
	for (;;) {
		c = (int)UpperToLower[*a] - (int)UpperToLower[*b];
		if (c || *a == 0)
			break;
		a++;
		b++;
	}
	return c;
}

int
sql_strnicmp(const char *zLeft, const char *zRight, int N)
{
	register unsigned char *a, *b;
	if (zLeft == 0) {
		return zRight ? -1 : 0;
	} else if (zRight == 0) {
		return 1;
	}
	a = (unsigned char *)zLeft;
	b = (unsigned char *)zRight;
	while (N-- > 0 && *a != 0 && UpperToLower[*a] == UpperToLower[*b]) {
		a++;
		b++;
	}
	return N < 0 ? 0 : UpperToLower[*a] - UpperToLower[*b];
}

/*
 * The string z[] is an text representation of a real number.
 * Convert this string to a double and write it into *pResult.
 *
 * The string z[] is length bytes in length (bytes, not characters) and
 * uses the encoding enc.  The string is not necessarily zero-terminated.
 *
 * Return TRUE if the result is a valid real number (or integer) and FALSE
 * if the string is empty or contains extraneous text.  Valid numbers
 * are in one of these formats:
 *
 *    [+-]digits[E[+-]digits]
 *    [+-]digits.[digits][E[+-]digits]
 *    [+-].digits[E[+-]digits]
 *
 * Leading and trailing whitespace is ignored for the purpose of determining
 * validity.
 *
 * If some prefix of the input string is a valid number, this routine
 * returns FALSE but it still converts the prefix and writes the result
 * into *pResult.
 */
int
sqlAtoF(const char *z, double *pResult, int length)
{
	int incr = 1; // UTF-8
	const char *zEnd = z + length;
	/* sign * significand * (10 ^ (esign * exponent)) */
	int sign = 1;		/* sign of significand */
	i64 s = 0;		/* significand */
	int d = 0;		/* adjust exponent for shifting decimal point */
	int esign = 1;		/* sign of exponent */
	int e = 0;		/* exponent */
	int eValid = 1;		/* True exponent is either not used or is well-formed */
	double result;
	int nDigits = 0;
	int nonNum = 0;		/* True if input contains UTF16 with high byte non-zero */

	*pResult = 0.0;		/* Default return value, in case of an error
	*/

	/* skip leading spaces */
	while (z < zEnd && sqlIsspace(*z))
		z += incr;
	if (z >= zEnd)
		return 0;

	/* get sign of significand */
	if (*z == '-') {
		sign = -1;
		z += incr;
	} else if (*z == '+') {
		z += incr;
	}

	/* copy max significant digits to significand */
	while (z < zEnd && sqlIsdigit(*z) && s < ((LARGEST_INT64 - 9) / 10)) {
		s = s * 10 + (*z - '0');
		z += incr, nDigits++;
	}

	/* skip non-significant significand digits
	 * (increase exponent by d to shift decimal left)
	 */
	while (z < zEnd && sqlIsdigit(*z))
		z += incr, nDigits++, d++;
	if (z >= zEnd)
		goto do_atof_calc;

	/* if decimal point is present */
	if (*z == '.') {
		z += incr;
		/* copy digits from after decimal to significand
		 * (decrease exponent by d to shift decimal right)
		 */
		while (z < zEnd && sqlIsdigit(*z)) {
			if (s < ((LARGEST_INT64 - 9) / 10)) {
				s = s * 10 + (*z - '0');
				d--;
			}
			z += incr, nDigits++;
		}
	}
	if (z >= zEnd)
		goto do_atof_calc;

	/* if exponent is present */
	if (*z == 'e' || *z == 'E') {
		z += incr;
		eValid = 0;

		/* This branch is needed to avoid a (harmless) buffer overread.  The
		 * special comment alerts the mutation tester that the correct answer
		 * is obtained even if the branch is omitted
		 */
		if (z >= zEnd)
			goto do_atof_calc;	/*PREVENTS-HARMLESS-OVERREAD */

		/* get sign of exponent */
		if (*z == '-') {
			esign = -1;
			z += incr;
		} else if (*z == '+') {
			z += incr;
		}
		/* copy digits to exponent */
		while (z < zEnd && sqlIsdigit(*z)) {
			e = e < 10000 ? (e * 10 + (*z - '0')) : 10000;
			z += incr;
			eValid = 1;
		}
	}

	/* skip trailing spaces */
	while (z < zEnd && sqlIsspace(*z))
		z += incr;

 do_atof_calc:
	/* adjust exponent by d, and update sign */
	e = (e * esign) + d;
	if (e < 0) {
		esign = -1;
		e *= -1;
	} else {
		esign = 1;
	}

	if (s == 0) {
		/* In the IEEE 754 standard, zero is signed. */
		result = sign < 0 ? -(double)0 : (double)0;
	} else {
		/* Attempt to reduce exponent.
		 *
		 * Branches that are not required for the correct answer but which only
		 * help to obtain the correct answer faster are marked with special
		 * comments, as a hint to the mutation tester.
		 */
		while (e > 0) {	/*OPTIMIZATION-IF-TRUE */
			if (esign > 0) {
				if (s >= (LARGEST_INT64 / 10))
					break;	/*OPTIMIZATION-IF-FALSE */
				s *= 10;
			} else {
				if (s % 10 != 0)
					break;	/*OPTIMIZATION-IF-FALSE */
				s /= 10;
			}
			e--;
		}

		/* adjust the sign of significand */
		s = sign < 0 ? -s : s;

		if (e == 0) {	/*OPTIMIZATION-IF-TRUE */
			result = (double)s;
		} else {
			LONGDOUBLE_TYPE scale = 1.0;
			/* attempt to handle extremely small/large numbers better */
			if (e > 307) {	/*OPTIMIZATION-IF-TRUE */
				if (e < 342) {	/*OPTIMIZATION-IF-TRUE */
					while (e % 308) {
						scale *= 1.0e+1;
						e -= 1;
					}
					if (esign < 0) {
						result = s / scale;
						result /= 1.0e+308;
					} else {
						result = s * scale;
						result *= 1.0e+308;
					}
				} else {
					assert(e >= 342);
					if (esign < 0) {
						result = 0.0 * s;
					} else {
						result = 1e308 * 1e308 * s;	/* Infinity */
					}
				}
			} else {
				/* 1.0e+22 is the largest power of 10 than can be
				 * represented exactly.
				 */
				while (e % 22) {
					scale *= 1.0e+1;
					e -= 1;
				}
				while (e > 0) {
					scale *= 1.0e+22;
					e -= 22;
				}
				if (esign < 0) {
					result = s / scale;
				} else {
					result = s * scale;
				}
			}
		}
	}

	/* store the result */
	*pResult = result;

	/* return true if number and no extra non-whitespace chracters after */
	return z == zEnd && nDigits > 0 && eValid && nonNum == 0;
}

/*
 * Compare the 19-character string zNum against the text representation
 * value 2^63:  9223372036854775808.  Return negative, zero, or positive
 * if zNum is less than, equal to, or greater than the string.
 * Note that zNum must contain exactly 19 characters.
 *
 * Unlike memcmp() this routine is guaranteed to return the difference
 * in the values of the last digit if the only difference is in the
 * last digit.  So, for example,
 *
 *      compare2pow63("9223372036854775800", 1)
 *
 * will return -8.
 */
static int
compare2pow63(const char *zNum, int incr)
{
	int c = 0;
	int i;
	/* 012345678901234567 */
	const char *pow63 = "922337203685477580";
	for (i = 0; c == 0 && i < 18; i++) {
		c = (zNum[i * incr] - pow63[i]) * 10;
	}
	if (c == 0) {
		c = zNum[18 * incr] - '8';
		testcase(c == (-1));
		testcase(c == 0);
		testcase(c == (+1));
	}
	return c;
}

int
sql_atoi64(const char *z, int64_t *val, int length)
{
	int incr = 1;
	u64 u = 0;
	int neg = 0;		/* assume positive */
	int i;
	int c = 0;
	int nonNum = 0;		/* True if input contains UTF16 with high byte non-zero */
	const char *zStart;
	const char *zEnd = z + length;
	incr = 1;
	while (z < zEnd && sqlIsspace(*z))
		z += incr;
	if (z < zEnd) {
		if (*z == '-') {
			neg = 1;
			z += incr;
		} else if (*z == '+') {
			z += incr;
		}
	}
	zStart = z;
	/* Skip leading zeros. */
	while (z < zEnd && z[0] == '0') {
		z += incr;
	}
	for (i = 0; &z[i] < zEnd && (c = z[i]) >= '0' && c <= '9';
	     i += incr) {
		u = u * 10 + c - '0';
	}
	if (u > LARGEST_INT64) {
		*val = neg ? SMALLEST_INT64 : LARGEST_INT64;
	} else if (neg) {
		*val = -(i64) u;
	} else {
		*val = (i64) u;
	}
	if (&z[i] < zEnd || (i == 0 && zStart == z) || i > 19 * incr ||
	    nonNum) {
		/* zNum is empty or contains non-numeric text or is longer
		 * than 19 digits (thus guaranteeing that it is too large)
		 */
		return 1;
	} else if (i < 19 * incr) {
		/* Less than 19 digits, so we know that it fits in 64 bits */
		assert(u <= LARGEST_INT64);
		return 0;
	} else {
		/* zNum is a 19-digit numbers.  Compare it against 9223372036854775808. */
		c = compare2pow63(z, incr);
		if (c < 0) {
			/* zNum is less than 9223372036854775808 so it fits */
			assert(u <= LARGEST_INT64);
			return 0;
		} else if (c > 0) {
			/* zNum is greater than 9223372036854775808 so it overflows */
			return 1;
		} else {
			/* zNum is exactly 9223372036854775808.  Fits if negative.  The
			 * special case 2 overflow if positive
			 */
			assert(u - 1 == LARGEST_INT64);
			return neg ? 0 : 2;
		}
	}
}

int
sql_dec_or_hex_to_i64(const char *z, int64_t *val)
{
	if (z[0] == '0' && (z[1] == 'x' || z[1] == 'X')) {
		uint64_t u = 0;
		int i, k;
		for (i = 2; z[i] == '0'; i++);
		for (k = i; sqlIsxdigit(z[k]); k++)
			u = u * 16 + sqlHexToInt(z[k]);
		memcpy(val, &u, 8);
		return (z[k] == 0 && k - i <= 16) ? 0 : 1;
	}
	return sql_atoi64(z, val, sqlStrlen30(z));
}

/*
 * If zNum represents an integer that will fit in 32-bits, then set
 * *pValue to that integer and return true.  Otherwise return false.
 *
 * This routine accepts both decimal and hexadecimal notation for integers.
 *
 * Any non-numeric characters that following zNum are ignored.
 * This is different from sqlAtoi64() which requires the
 * input number to be zero-terminated.
 */
int
sqlGetInt32(const char *zNum, int *pValue)
{
	sql_int64 v = 0;
	int i, c;
	int neg = 0;
	if (zNum[0] == '-') {
		neg = 1;
		zNum++;
	} else if (zNum[0] == '+') {
		zNum++;
	}
	else if (zNum[0] == '0' && (zNum[1] == 'x' || zNum[1] == 'X')
		 && sqlIsxdigit(zNum[2])
	    ) {
		u32 u = 0;
		zNum += 2;
		while (zNum[0] == '0')
			zNum++;
		for (i = 0; sqlIsxdigit(zNum[i]) && i < 8; i++) {
			u = u * 16 + sqlHexToInt(zNum[i]);
		}
		if ((u & 0x80000000) == 0 && sqlIsxdigit(zNum[i]) == 0) {
			memcpy(pValue, &u, 4);
			return 1;
		} else {
			return 0;
		}
	}
	while (zNum[0] == '0')
		zNum++;
	for (i = 0; i < 11 && (c = zNum[i] - '0') >= 0 && c <= 9; i++) {
		v = v * 10 + c;
	}

	/* The longest decimal representation of a 32 bit integer is 10 digits:
	 *
	 *             1234567890
	 *     2^31 -> 2147483648
	 */
	testcase(i == 10);
	if (i > 10) {
		return 0;
	}
	testcase(v - neg == 2147483647);
	if (v - neg > 2147483647) {
		return 0;
	}
	if (neg) {
		v = -v;
	}
	*pValue = (int)v;
	return 1;
}

/*
 * Return a 32-bit integer value extracted from a string.  If the
 * string is not an integer, just return 0.
 */
int
sqlAtoi(const char *z)
{
	int x = 0;
	if (z)
		sqlGetInt32(z, &x);
	return x;
}

/*
 * The variable-length integer encoding is as follows:
 *
 * KEY:
 *         A = 0xxxxxxx    7 bits of data and one flag bit
 *         B = 1xxxxxxx    7 bits of data and one flag bit
 *         C = xxxxxxxx    8 bits of data
 *
 *  7 bits - A
 * 14 bits - BA
 * 21 bits - BBA
 * 28 bits - BBBA
 * 35 bits - BBBBA
 * 42 bits - BBBBBA
 * 49 bits - BBBBBBA
 * 56 bits - BBBBBBBA
 * 64 bits - BBBBBBBBC
 */

/*
 * Write a 64-bit variable-length integer to memory starting at p[0].
 * The length of data write will be between 1 and 9 bytes.  The number
 * of bytes written is returned.
 *
 * A variable-length integer consists of the lower 7 bits of each byte
 * for all bytes that have the 8th bit set and one byte with the 8th
 * bit clear.  Except, if we get to the 9th byte, it stores the full
 * 8 bits and is the last byte.
 */
static int SQL_NOINLINE
putVarint64(unsigned char *p, u64 v)
{
	int i, j, n;
	u8 buf[10];
	if (v & (((u64) 0xff000000) << 32)) {
		p[8] = (u8) v;
		v >>= 8;
		for (i = 7; i >= 0; i--) {
			p[i] = (u8) ((v & 0x7f) | 0x80);
			v >>= 7;
		}
		return 9;
	}
	n = 0;
	do {
		buf[n++] = (u8) ((v & 0x7f) | 0x80);
		v >>= 7;
	} while (v != 0);
	buf[0] &= 0x7f;
	assert(n <= 9);
	for (i = 0, j = n - 1; j >= 0; j--, i++) {
		p[i] = buf[j];
	}
	return n;
}

int
sqlPutVarint(unsigned char *p, u64 v)
{
	if (v <= 0x7f) {
		p[0] = v & 0x7f;
		return 1;
	}
	if (v <= 0x3fff) {
		p[0] = ((v >> 7) & 0x7f) | 0x80;
		p[1] = v & 0x7f;
		return 2;
	}
	return putVarint64(p, v);
}

/*
 * Bitmasks used by sqlGetVarint().  These precomputed constants
 * are defined here rather than simply putting the constant expressions
 * inline in order to work around bugs in the RVT compiler.
 *
 * SLOT_2_0     A mask for  (0x7f<<14) | 0x7f
 *
 * SLOT_4_2_0   A mask for  (0x7f<<28) | SLOT_2_0
 */
#define SLOT_2_0     0x001fc07f
#define SLOT_4_2_0   0xf01fc07f

/*
 * Read a 64-bit variable-length integer from memory starting at p[0].
 * Return the number of bytes read.  The value is stored in *v.
 */
u8
sqlGetVarint(const unsigned char *p, u64 * v)
{
	u32 a, b, s;

	a = *p;
	/* a: p0 (unmasked) */
	if (!(a & 0x80)) {
		*v = a;
		return 1;
	}

	p++;
	b = *p;
	/* b: p1 (unmasked) */
	if (!(b & 0x80)) {
		a &= 0x7f;
		a = a << 7;
		a |= b;
		*v = a;
		return 2;
	}

	/* Verify that constants are precomputed correctly */
	assert(SLOT_2_0 == ((0x7f << 14) | (0x7f)));
	assert(SLOT_4_2_0 == ((0xfU << 28) | (0x7f << 14) | (0x7f)));

	p++;
	a = a << 14;
	a |= *p;
	/* a: p0<<14 | p2 (unmasked) */
	if (!(a & 0x80)) {
		a &= SLOT_2_0;
		b &= 0x7f;
		b = b << 7;
		a |= b;
		*v = a;
		return 3;
	}

	/* CSE1 from below */
	a &= SLOT_2_0;
	p++;
	b = b << 14;
	b |= *p;
	/* b: p1<<14 | p3 (unmasked) */
	if (!(b & 0x80)) {
		b &= SLOT_2_0;
		/* moved CSE1 up */
		/* a &= (0x7f<<14)|(0x7f); */
		a = a << 7;
		a |= b;
		*v = a;
		return 4;
	}

	/* a: p0<<14 | p2 (masked) */
	/* b: p1<<14 | p3 (unmasked) */
	/* 1:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
	/* moved CSE1 up */
	/* a &= (0x7f<<14)|(0x7f); */
	b &= SLOT_2_0;
	s = a;
	/* s: p0<<14 | p2 (masked) */

	p++;
	a = a << 14;
	a |= *p;
	/* a: p0<<28 | p2<<14 | p4 (unmasked) */
	if (!(a & 0x80)) {
		/* we can skip these cause they were (effectively) done above
		 * while calculating s
		 */
		/* a &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
		/* b &= (0x7f<<14)|(0x7f); */
		b = b << 7;
		a |= b;
		s = s >> 18;
		*v = ((u64) s) << 32 | a;
		return 5;
	}

	/* 2:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
	s = s << 7;
	s |= b;
	/* s: p0<<21 | p1<<14 | p2<<7 | p3 (masked) */

	p++;
	b = b << 14;
	b |= *p;
	/* b: p1<<28 | p3<<14 | p5 (unmasked) */
	if (!(b & 0x80)) {
		/* we can skip this cause it was (effectively) done above in calc'ing s */
		/* b &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
		a &= SLOT_2_0;
		a = a << 7;
		a |= b;
		s = s >> 18;
		*v = ((u64) s) << 32 | a;
		return 6;
	}

	p++;
	a = a << 14;
	a |= *p;
	/* a: p2<<28 | p4<<14 | p6 (unmasked) */
	if (!(a & 0x80)) {
		a &= SLOT_4_2_0;
		b &= SLOT_2_0;
		b = b << 7;
		a |= b;
		s = s >> 11;
		*v = ((u64) s) << 32 | a;
		return 7;
	}

	/* CSE2 from below */
	a &= SLOT_2_0;
	p++;
	b = b << 14;
	b |= *p;
	/* b: p3<<28 | p5<<14 | p7 (unmasked) */
	if (!(b & 0x80)) {
		b &= SLOT_4_2_0;
		/* moved CSE2 up */
		/* a &= (0x7f<<14)|(0x7f); */
		a = a << 7;
		a |= b;
		s = s >> 4;
		*v = ((u64) s) << 32 | a;
		return 8;
	}

	p++;
	a = a << 15;
	a |= *p;
	/* a: p4<<29 | p6<<15 | p8 (unmasked) */

	/* moved CSE2 up */
	/* a &= (0x7f<<29)|(0x7f<<15)|(0xff); */
	b &= SLOT_2_0;
	b = b << 8;
	a |= b;

	s = s << 4;
	b = p[-4];
	b &= 0x7f;
	b = b >> 3;
	s |= b;

	*v = ((u64) s) << 32 | a;

	return 9;
}

/*
 * Read a 32-bit variable-length integer from memory starting at p[0].
 * Return the number of bytes read.  The value is stored in *v.
 *
 * If the varint stored in p[0] is larger than can fit in a 32-bit unsigned
 * integer, then set *v to 0xffffffff.
 *
 * A MACRO version, getVarint32, is provided which inlines the
 * single-byte case.  All code should use the MACRO version as
 * this function assumes the single-byte case has already been handled.
 */
u8
sqlGetVarint32(const unsigned char *p, u32 * v)
{
	u32 a, b;

	/* The 1-byte case.  Overwhelmingly the most common.  Handled inline
	 * by the getVarin32() macro
	 */
	a = *p;
	/* a: p0 (unmasked) */
#ifndef getVarint32
	if (!(a & 0x80)) {
		/* Values between 0 and 127 */
		*v = a;
		return 1;
	}
#endif

	/* The 2-byte case */
	p++;
	b = *p;
	/* b: p1 (unmasked) */
	if (!(b & 0x80)) {
		/* Values between 128 and 16383 */
		a &= 0x7f;
		a = a << 7;
		*v = a | b;
		return 2;
	}

	/* The 3-byte case */
	p++;
	a = a << 14;
	a |= *p;
	/* a: p0<<14 | p2 (unmasked) */
	if (!(a & 0x80)) {
		/* Values between 16384 and 2097151 */
		a &= (0x7f << 14) | (0x7f);
		b &= 0x7f;
		b = b << 7;
		*v = a | b;
		return 3;
	}

	/* A 32-bit varint is used to store size information in btrees.
	 * Objects are rarely larger than 2MiB limit of a 3-byte varint.
	 * A 3-byte varint is sufficient, for example, to record the size
	 * of a 1048569-byte BLOB or string.
	 *
	 * We only unroll the first 1-, 2-, and 3- byte cases.  The very
	 * rare larger cases can be handled by the slower 64-bit varint
	 * routine.
	 */
	{
		u64 v64;
		u8 n;

		p -= 2;
		n = sqlGetVarint(p, &v64);
		assert(n > 3 && n <= 9);
		if ((v64 & SQL_MAX_U32) != v64) {
			*v = 0xffffffff;
		} else {
			*v = (u32) v64;
		}
		return n;
	}
}

/*
 * Return the number of bytes that will be needed to store the given
 * 64-bit integer.
 */
int
sqlVarintLen(u64 v)
{
	int i;
	for (i = 1; (v >>= 7) != 0; i++) {
		assert(i < 10);
	}
	return i;
}

/*
 * Read or write a four-byte big-endian integer value.
 */
u32
sqlGet4byte(const u8 * p)
{
#if SQL_BYTEORDER==4321
	u32 x;
	memcpy(&x, p, 4);
	return x;
#elif SQL_BYTEORDER==1234 && !defined(SQL_DISABLE_INTRINSIC) \
    && defined(__GNUC__) && GCC_VERSION>=4003000
	u32 x;
	memcpy(&x, p, 4);
	return __builtin_bswap32(x);
#elif SQL_BYTEORDER==1234 && !defined(SQL_DISABLE_INTRINSIC) \
    && defined(_MSC_VER) && _MSC_VER>=1300
	u32 x;
	memcpy(&x, p, 4);
	return _byteswap_ulong(x);
#else
	testcase(p[0] & 0x80);
	return ((unsigned)p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
#endif
}

void
sqlPut4byte(unsigned char *p, u32 v)
{
#if SQL_BYTEORDER==4321
	memcpy(p, &v, 4);
#elif SQL_BYTEORDER==1234 && !defined(SQL_DISABLE_INTRINSIC) \
    && defined(__GNUC__) && GCC_VERSION>=4003000
	u32 x = __builtin_bswap32(v);
	memcpy(p, &x, 4);
#elif SQL_BYTEORDER==1234 && !defined(SQL_DISABLE_INTRINSIC) \
    && defined(_MSC_VER) && _MSC_VER>=1300
	u32 x = _byteswap_ulong(v);
	memcpy(p, &x, 4);
#else
	p[0] = (u8) (v >> 24);
	p[1] = (u8) (v >> 16);
	p[2] = (u8) (v >> 8);
	p[3] = (u8) v;
#endif
}

/*
 * Translate a single byte of Hex into an integer.
 * This routine only works if h really is a valid hexadecimal
 * character:  0..9a..fA..F
 */
u8
sqlHexToInt(int h)
{
	assert((h >= '0' && h <= '9') || (h >= 'a' && h <= 'f')
	       || (h >= 'A' && h <= 'F'));
	h += 9 * (1 & (h >> 6));
	return (u8) (h & 0xf);
}

#if !defined(SQL_OMIT_BLOB_LITERAL) || defined(SQL_HAS_CODEC)
/*
 * Convert a BLOB literal of the form "x'hhhhhh'" into its binary
 * value.  Return a pointer to its binary value.  Space to hold the
 * binary value has been obtained from malloc and must be freed by
 * the calling routine.
 */
void *
sqlHexToBlob(sql * db, const char *z, int n)
{
	char *zBlob;
	int i;

	zBlob = (char *)sqlDbMallocRawNN(db, n / 2 + 1);
	n--;
	if (zBlob) {
		for (i = 0; i < n; i += 2) {
			zBlob[i / 2] =
			    (sqlHexToInt(z[i]) << 4) |
			    sqlHexToInt(z[i + 1]);
		}
		zBlob[i / 2] = 0;
	}
	return zBlob;
}
#endif				/* !SQL_OMIT_BLOB_LITERAL || SQL_HAS_CODEC */

/*
 * Log an error that is an API call on a connection pointer that should
 * not have been used.  The "type" of connection pointer is given as the
 * argument.  The zType is a word like "NULL" or "closed" or "invalid".
 */
static void
logBadConnection(const char *zType)
{
	sql_log(SQL_MISUSE,
		    "API call with %s database connection pointer", zType);
}

/*
 * Check to make sure we have a valid db pointer.  This test is not
 * foolproof but it does provide some measure of protection against
 * misuse of the interface such as passing in db pointers that are
 * NULL or which have been previously closed.  If this routine returns
 * 1 it means that the db pointer is valid and 0 if it should not be
 * dereferenced for any reason.  The calling function should invoke
 * SQL_MISUSE immediately.
 *
 * sqlSafetyCheckOk() requires that the db pointer be valid for
 * use.  sqlSafetyCheckSickOrOk() allows a db pointer that failed to
 * open properly and is not fit for general use but which can be
 * used as an argument to sql_errmsg() or sql_close().
 */
int
sqlSafetyCheckOk(sql * db)
{
	u32 magic;
	if (db == 0) {
		logBadConnection("NULL");
		return 0;
	}
	magic = db->magic;
	if (magic != SQL_MAGIC_OPEN) {
		if (sqlSafetyCheckSickOrOk(db)) {
			testcase(sqlGlobalConfig.xLog != 0);
			logBadConnection("unopened");
		}
		return 0;
	} else {
		return 1;
	}
}

int
sqlSafetyCheckSickOrOk(sql * db)
{
	u32 magic;
	magic = db->magic;
	if (magic != SQL_MAGIC_SICK &&
	    magic != SQL_MAGIC_OPEN && magic != SQL_MAGIC_BUSY) {
		testcase(sqlGlobalConfig.xLog != 0);
		logBadConnection("invalid");
		return 0;
	} else {
		return 1;
	}
}

/*
 * Attempt to add, substract, or multiply the 64-bit signed value iB against
 * the other 64-bit signed integer at *pA and store the result in *pA.
 * Return 0 on success.  Or if the operation would have resulted in an
 * overflow, leave *pA unchanged and return 1.
 */
int
sqlAddInt64(i64 * pA, i64 iB)
{
	i64 iA = *pA;
	testcase(iA == 0);
	testcase(iA == 1);
	testcase(iB == -1);
	testcase(iB == 0);
	if (iB >= 0) {
		testcase(iA > 0 && LARGEST_INT64 - iA == iB);
		testcase(iA > 0 && LARGEST_INT64 - iA == iB - 1);
		if (iA > 0 && LARGEST_INT64 - iA < iB)
			return 1;
	} else {
		testcase(iA < 0 && -(iA + LARGEST_INT64) == iB + 1);
		testcase(iA < 0 && -(iA + LARGEST_INT64) == iB + 2);
		if (iA < 0 && -(iA + LARGEST_INT64) > iB + 1)
			return 1;
	}
	*pA += iB;
	return 0;
}

int
sqlSubInt64(i64 * pA, i64 iB)
{
	testcase(iB == SMALLEST_INT64 + 1);
	if (iB == SMALLEST_INT64) {
		testcase((*pA) == (-1));
		testcase((*pA) == 0);
		if ((*pA) >= 0)
			return 1;
		*pA -= iB;
		return 0;
	} else {
		return sqlAddInt64(pA, -iB);
	}
}

int
sqlMulInt64(i64 * pA, i64 iB)
{
	i64 iA = *pA;
	if (iB > 0) {
		if (iA > LARGEST_INT64 / iB)
			return 1;
		if (iA < SMALLEST_INT64 / iB)
			return 1;
	} else if (iB < 0) {
		if (iA > 0) {
			if (iB < SMALLEST_INT64 / iA)
				return 1;
		} else if (iA < 0) {
			if (iB == SMALLEST_INT64)
				return 1;
			if (iA == SMALLEST_INT64)
				return 1;
			if (-iA > LARGEST_INT64 / -iB)
				return 1;
		}
	}
	*pA = iA * iB;
	return 0;
}

/*
 * Compute the absolute value of a 32-bit signed integer, of possible.  Or
 * if the integer has a value of -2147483648, return +2147483647
 */
int
sqlAbsInt32(int x)
{
	if (x >= 0)
		return x;
	if (x == (int)0x80000000)
		return 0x7fffffff;
	return -x;
}

/*
 * Find (an approximate) sum of two LogEst values.  This computation is
 * not a simple "+" operator because LogEst is stored as a logarithmic
 * value.
 *
 */
LogEst
sqlLogEstAdd(LogEst a, LogEst b)
{
	static const unsigned char x[] = {
		10, 10,		/* 0,1 */
		9, 9,		/* 2,3 */
		8, 8,		/* 4,5 */
		7, 7, 7,	/* 6,7,8 */
		6, 6, 6,	/* 9,10,11 */
		5, 5, 5,	/* 12-14 */
		4, 4, 4, 4,	/* 15-18 */
		3, 3, 3, 3, 3, 3,	/* 19-24 */
		2, 2, 2, 2, 2, 2, 2,	/* 25-31 */
	};
	if (a >= b) {
		if (a > b + 49)
			return a;
		if (a > b + 31)
			return a + 1;
		return a + x[a - b];
	} else {
		if (b > a + 49)
			return b;
		if (b > a + 31)
			return b + 1;
		return b + x[b - a];
	}
}

/*
 * Convert an integer into a LogEst.  In other words, compute an
 * approximation for 10*log2(x).
 */
LogEst
sqlLogEst(u64 x)
{
	static LogEst a[] = { 0, 2, 3, 5, 6, 7, 8, 9 };
	LogEst y = 40;
	if (x < 8) {
		if (x < 2)
			return 0;
		while (x < 8) {
			y -= 10;
			x <<= 1;
		}
	} else {
		while (x > 255) {
			y += 40;
			x >>= 4;
		}		/*OPTIMIZATION-IF-TRUE */
		while (x > 15) {
			y += 10;
			x >>= 1;
		}
	}
	return a[x & 7] + y - 10;
}

/*
 * Convert a LogEst into an integer.
 *
 * Note that this routine is only used when one or more of various
 * non-standard compile-time options is enabled.
 */
u64
sqlLogEstToInt(LogEst x)
{
	u64 n;
	n = x % 10;
	x /= 10;
	if (n >= 5)
		n -= 2;
	else if (n >= 1)
		n -= 1;
#if defined(SQL_ENABLE_STMT_SCANSTATUS) || \
    defined(SQL_EXPLAIN_ESTIMATED_ROWS)
	if (x > 60)
		return (u64) LARGEST_INT64;
#else
	/* The largest input possible to this routine is 310,
	 * resulting in a maximum x of 31
	 */
	assert(x <= 60);
#endif
	return x >= 3 ? (n + 8) << (x - 3) : (n + 8) >> (3 - x);
}

/*
 * Add a new name/number pair to a VList.  This might require that the
 * VList object be reallocated, so return the new VList.  If an OOM
 * error occurs, the original VList returned and the
 * db->mallocFailed flag is set.
 *
 * A VList is really just an array of integers.  To destroy a VList,
 * simply pass it to sqlDbFree().
 *
 * The first integer is the number of integers allocated for the whole
 * VList.  The second integer is the number of integers actually used.
 * Each name/number pair is encoded by subsequent groups of 3 or more
 * integers.
 *
 * Each name/number pair starts with two integers which are the numeric
 * value for the pair and the size of the name/number pair, respectively.
 * The text name overlays one or more following integers.  The text name
 * is always zero-terminated.
 *
 * Conceptually:
 *
 *    struct VList {
 *      int nAlloc;   // Number of allocated slots
 *      int nUsed;    // Number of used slots
 *      struct VListEntry {
 *        int iValue;    // Value for this entry
 *        int nSlot;     // Slots used by this entry
 *        // ... variable name goes here
 *      } a[0];
 *    }
 *
 * During code generation, pointers to the variable names within the
 * VList are taken.  When that happens, nAlloc is set to zero as an
 * indication that the VList may never again be enlarged, since the
 * accompanying realloc() would invalidate the pointers.
 */
VList *
sqlVListAdd(sql * db,	/* The database connection used for malloc() */
		VList * pIn,	/* The input VList.  Might be NULL */
		const char *zName,	/* Name of symbol to add */
		int nName,	/* Bytes of text in zName */
		int iVal	/* Value to associate with zName */
    )
{
	int nInt;		/* number of sizeof(int) objects needed for zName */
	char *z;		/* Pointer to where zName will be stored */
	int i;			/* Index in pIn[] where zName is stored */

	nInt = nName / 4 + 3;
	assert(pIn == 0 || pIn[0] >= 3);	/* Verify ok to add new elements */
	if (pIn == 0 || pIn[1] + nInt > pIn[0]) {
		/* Enlarge the allocation */
		int nAlloc = (pIn ? pIn[0] * 2 : 10) + nInt;
		VList *pOut = sqlDbRealloc(db, pIn, nAlloc * sizeof(int));
		if (pOut == 0)
			return pIn;
		if (pIn == 0)
			pOut[1] = 2;
		pIn = pOut;
		pIn[0] = nAlloc;
	}
	i = pIn[1];
	pIn[i] = iVal;
	pIn[i + 1] = nInt;
	z = (char *)&pIn[i + 2];
	pIn[1] = i + nInt;
	assert(pIn[1] <= pIn[0]);
	memcpy(z, zName, nName);
	z[nName] = 0;
	return pIn;
}

/*
 * Return a pointer to the name of a variable in the given VList that
 * has the value iVal.  Or return a NULL if there is no such variable in
 * the list
 */
const char *
sqlVListNumToName(VList * pIn, int iVal)
{
	int i, mx;
	if (pIn == 0)
		return 0;
	mx = pIn[1];
	i = 2;
	do {
		if (pIn[i] == iVal)
			return (char *)&pIn[i + 2];
		i += pIn[i + 1];
	} while (i < mx);
	return 0;
}

/*
 * Return the number of the variable named zName, if it is in VList.
 * or return 0 if there is no such variable.
 */
int
sqlVListNameToNum(VList * pIn, const char *zName, int nName)
{
	int i, mx;
	if (pIn == 0)
		return 0;
	mx = pIn[1];
	i = 2;
	do {
		const char *z = (const char *)&pIn[i + 2];
		if (strncmp(z, zName, nName) == 0 && z[nName] == 0)
			return pIn[i];
		i += pIn[i + 1];
	} while (i < mx);
	return 0;
}
