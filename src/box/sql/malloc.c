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
 *
 * Memory allocation functions used throughout sql.
 */
#include "sqlInt.h"
#include <stdarg.h>

/*
 * Like malloc(), but remember the size of the allocation
 * so that we can find it later using sqlMemSize().
 *
 * For this low-level routine, we are guaranteed that nByte>0 because
 * cases of nByte<=0 will be intercepted and dealt with by higher level
 * routines.
 */
static void *
sql_sized_malloc(int nByte)
{
	sql_int64 *p;
	assert(nByte > 0);
	nByte = ROUND8(nByte);
	p = malloc(nByte + 8);
	if (p == NULL) {
		sql_get()->mallocFailed = 1;
		diag_set(OutOfMemory, nByte, "malloc", "p");
		return NULL;
	}
	p[0] = nByte;
	p++;
	return (void *)p;
}

/*
 * Report the allocated size of a prior return from sql_sized_malloc()
 * or sql_sized_realloc().
 */
static int
sql_sized_sizeof(void *pPrior)
{
	sql_int64 *p;
	assert(pPrior != 0);
	p = (sql_int64 *) pPrior;
	p--;
	return (int)p[0];
}

/*
 * Like realloc().  Resize an allocation previously obtained from
 * sqlMemMalloc().
 *
 * For this low-level interface, we know that pPrior!=0.  Cases where
 * pPrior==0 while have been intercepted by higher-level routine and
 * redirected to sql_sized_malloc.  Similarly, we know that nByte>0 because
 * cases where nByte<=0 will have been intercepted by higher-level
 * routines and redirected to sql_sized_free.
 */
static void *
sql_sized_realloc(void *pPrior, int nByte)
{
	sql_int64 *p = (sql_int64 *) pPrior;
	assert(pPrior != 0 && nByte > 0);
	assert(nByte == ROUND8(nByte));	/* EV: R-46199-30249 */
	p--;
	p = realloc(p, nByte + 8);
	if (p == NULL) {
		sql_get()->mallocFailed = 1;
		diag_set(OutOfMemory, nByte, "realloc", "p");
		return NULL;
	}
	p[0] = nByte;
	p++;
	return (void *)p;
}

/*
 * Allocate memory.  This routine is like sql_malloc() except that it
 * assumes the memory subsystem has already been initialized.
 */
void *
sqlMalloc(u64 n)
{
	void *p;
	if (n == 0 || n >= 0x7fffff00) {
		/* A memory allocation of a number of bytes which is near the maximum
		 * signed integer value might cause an integer overflow inside of the
		 * sql_sized_malloc().  Hence we limit the maximum size to 0x7fffff00, giving
		 * 255 bytes of overhead.  sql itself will never use anything near
		 * this amount.  The only way to reach the limit is with sql_malloc()
		 */
		p = 0;
	} else {
		p = sql_sized_malloc((int)n);
	}
	assert(EIGHT_BYTE_ALIGNMENT(p));	/* IMP: R-11148-40995 */
	return p;
}

/*
 * Return the size of a memory allocation previously obtained from
 * sqlMalloc() or sql_malloc().
 */
int
sqlMallocSize(void *p)
{
	return sql_sized_sizeof(p);
}

/*
 * Free memory previously obtained from sqlMalloc().
 */
void
sql_free(void *p)
{
	if (p == NULL)
		return;
	sql_int64 *raw_p = (sql_int64 *) p;
	raw_p--;
	free(raw_p);
}

/*
 * Change the size of an existing memory allocation
 */
void *
sqlRealloc(void *pOld, u64 nBytes)
{
	int nOld, nNew;
	void *pNew;
	if (pOld == 0) {
		return sqlMalloc(nBytes);	/* IMP: R-04300-56712 */
	}
	if (nBytes == 0) {
		sql_free(pOld);	/* IMP: R-26507-47431 */
		return 0;
	}
	if (nBytes >= 0x7fffff00) {
		/* The 0x7ffff00 limit term is explained in comments on sqlMalloc() */
		return 0;
	}
	nOld = sqlMallocSize(pOld);
	nNew = ROUND8((int)nBytes);
	if (nOld == nNew)
		pNew = pOld;
	else
		pNew = sql_sized_realloc(pOld, nNew);
	assert(EIGHT_BYTE_ALIGNMENT(pNew));	/* IMP: R-11148-40995 */
	return pNew;
}

/*
 * Allocate and zero memory.
 */
void *
sqlMallocZero(u64 n)
{
	void *p = sqlMalloc(n);
	if (p) {
		memset(p, 0, (size_t) n);
	}
	return p;
}

/*
 * Allocate and zero memory.  If the allocation fails, make
 * the mallocFailed flag in the connection pointer.
 */
void *
sqlDbMallocZero(sql * db, u64 n)
{
	void *p;
	testcase(db == 0);
	p = sqlDbMallocRaw(db, n);
	if (p)
		memset(p, 0, (size_t) n);
	return p;
}

/*
 * Allocate heap memory. If the allocation fails, set the
 * mallocFailed flag in the connection pointer.
 *
 * If db!=0 and db->mallocFailed is true (indicating a prior malloc
 * failure on the same database connection) then always return 0.
 * Hence for a particular database connection, once malloc starts
 * failing, it fails consistently until mallocFailed is reset.
 * This is an important assumption.  There are many places in the
 * code that do things like this:
 *
 *         int *a = (int*)sqlDbMallocRaw(db, 100);
 *         int *b = (int*)sqlDbMallocRaw(db, 200);
 *         if( b ) a[10] = 9;
 *
 * In other words, if a subsequent malloc (ex: "b") worked, it is assumed
 * that all prior mallocs (ex: "a") worked too.
 *
 * The sqlMallocRawNN() variant guarantees that the "db" parameter is
 * not a NULL pointer.
 */
void *
sqlDbMallocRaw(sql * db, u64 n)
{
	void *p;
	if (db)
		return sqlDbMallocRawNN(db, n);
	p = sqlMalloc(n);
	return p;
}

void *
sqlDbMallocRawNN(sql * db, u64 n)
{
	assert(db != NULL);
	if (db->mallocFailed)
		return NULL;
	void *p = sqlMalloc(n);
	if (p == NULL)
		sqlOomFault(db);
	return p;
}

/* Forward declaration */
static SQL_NOINLINE void *dbReallocFinish(sql * db, void *p, u64 n);

/*
 * Resize the block of memory pointed to by p to n bytes. If the
 * resize fails, set the mallocFailed flag in the connection object.
 */
void *
sqlDbRealloc(sql * db, void *p, u64 n)
{
	assert(db != 0);
	if (p == 0)
		return sqlDbMallocRawNN(db, n);
	return dbReallocFinish(db, p, n);
}

static SQL_NOINLINE void *
dbReallocFinish(sql * db, void *p, u64 n)
{
	void *pNew = 0;
	assert(db != 0);
	assert(p != 0);
	if (db->mallocFailed == 0) {
		pNew = sqlRealloc(p, n);
		if (!pNew)
			sqlOomFault(db);
	}
	return pNew;
}

/*
 * Attempt to reallocate p.  If the reallocation fails, then free p
 * and set the mallocFailed flag in the database connection.
 */
void *
sqlDbReallocOrFree(sql * db, void *p, u64 n)
{
	void *pNew;
	pNew = sqlDbRealloc(db, p, n);
	if (!pNew)
		sql_free(p);
	return pNew;
}

/*
 * Make a copy of a string in memory obtained from sqlMalloc(). These
 * functions call sqlMallocRaw() directly instead of sqlMalloc(). This
 * is because when memory debugging is turned on, these two functions are
 * called via macros that record the current file and line number in the
 * ThreadData structure.
 */
char *
sqlDbStrDup(sql * db, const char *z)
{
	char *zNew;
	size_t n;
	if (z == 0) {
		return 0;
	}
	n = strlen(z) + 1;
	zNew = sqlDbMallocRaw(db, n);
	if (zNew) {
		memcpy(zNew, z, n);
	}
	return zNew;
}

char *
sqlDbStrNDup(sql * db, const char *z, u64 n)
{
	char *zNew;
	assert(db != 0);
	if (z == 0) {
		return 0;
	}
	assert((n & 0x7fffffff) == n);
	zNew = sqlDbMallocRawNN(db, n + 1);
	if (zNew) {
		memcpy(zNew, z, (size_t) n);
		zNew[n] = 0;
	}
	return zNew;
}

/*
 * This routine reactivates the memory allocator and clears the
 * db->mallocFailed flag as necessary.
 *
 * The memory allocator is not restarted if there are running
 * VDBEs.
 */
void
sqlOomClear(sql * db)
{
	if (db->mallocFailed && db->nVdbeExec == 0)
		db->mallocFailed = 0;
}
