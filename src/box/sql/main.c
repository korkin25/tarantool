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
 * Main file for the sql library.  The routines in this file
 * implement the programmer interface to the library.  Routines in
 * other files are for internal use by sql and should not be
 * accessed by users of the library.
 */
#include "sqlInt.h"
#include "vdbeInt.h"
#include "version.h"
#include "box/session.h"

#if !defined(SQL_OMIT_TRACE) && defined(SQL_ENABLE_IOTRACE)
/*
 * If the following function pointer is not NULL and if
 * SQL_ENABLE_IOTRACE is enabled, then messages describing
 * I/O active are written using this function.  These messages
 * are intended for debugging activity only.
 */
SQL_API void (SQL_CDECL * sqlIoTrace) (const char *, ...) = 0;
#endif

/*
 * If the following global variable points to a string which is the
 * name of a directory, then that directory will be used to store
 * temporary files.
 *
 * See also the "PRAGMA temp_store_directory" SQL command.
 */
char *sql_temp_directory = 0;

/*
 * If the following global variable points to a string which is the
 * name of a directory, then that directory will be used to store
 * all database files specified with a relative pathname.
 *
 * See also the "PRAGMA data_store_directory" SQL command.
 */
char *sql_data_directory = 0;

/*
 * Initialize sql.
 *
 * This routine must be called to initialize the memory allocation,
 * and VFS subsystems prior to doing any serious work with
 * sql.
 *
 * This routine is a no-op except on its very first call for the process.
 *
 * The first thread to call this routine runs the initialization to
 * completion.  If subsequent threads call this routine before the first
 * thread has finished the initialization process, then the subsequent
 * threads must block until the first thread finishes with the initialization.
 *
 * The first thread might call this routine recursively.  Recursive
 * calls to this routine should not block, of course.  Otherwise the
 * initialization process would never complete.
 *
 * Let X be the first thread to enter this routine.  Let Y be some other
 * thread.  Then while the initial invocation of this routine by X is
 * incomplete, it is required that:
 *
 *    *  Calls to this routine from Y must block until the outer-most
 *       call by X completes.
 *
 *    *  Recursive calls to this routine from thread X return immediately
 *       without blocking.
 */
int
sql_initialize(void)
{
	int rc = SQL_OK;

	/* If the following assert() fails on some obscure processor/compiler
	 * combination, the work-around is to set the correct pointer
	 * size at compile-time using -DSQL_PTRSIZE=n compile-time option
	 */
	assert(SQL_PTRSIZE == sizeof(char *));

	/* If sql is already completely initialized, then this call
	 * to sql_initialize() should be a no-op.  But the initialization
	 * must be complete.  So isInit must not be set until the very end
	 * of this routine.
	 */
	if (sqlGlobalConfig.isInit)
		return SQL_OK;

	if (!sqlGlobalConfig.isMallocInit)
		sqlMallocInit();
	if (rc == SQL_OK)
		sqlGlobalConfig.isMallocInit = 1;

	/* If rc is not SQL_OK at this point, then the malloc
	 * subsystem could not be initialized.
	 */
	if (rc != SQL_OK)
		return rc;

	/* Do the rest of the initialization
	 * that we will be able to handle recursive calls into
	 * sql_initialize().  The recursive calls normally come through
	 * sql_os_init() when it invokes sql_vfs_register(), but other
	 * recursive calls might also be possible.
	 *
	 * IMPLEMENTATION-OF: R-00140-37445 sql automatically serializes calls
	 * to the xInit method, so the xInit method need not be threadsafe.
	 *
         * The sql_pcache_methods.xInit() all is embedded in the
	 * call to sqlPcacheInitialize().
	 */
	if (sqlGlobalConfig.isInit == 0
	    && sqlGlobalConfig.inProgress == 0) {
		sqlGlobalConfig.inProgress = 1;
		memset(&sqlBuiltinFunctions, 0,
		       sizeof(sqlBuiltinFunctions));
		sqlRegisterBuiltinFunctions();
		if (rc == SQL_OK) {
			rc = sqlOsInit();
		}
		if (rc == SQL_OK) {
			sqlGlobalConfig.isInit = 1;
		}
		sqlGlobalConfig.inProgress = 0;
	}

	/* The following is just a sanity check to make sure sql has
	 * been compiled correctly.  It is important to run this code, but
	 * we don't want to run it too often and soak up CPU cycles for no
	 * reason.  So we run it once during initialization.
	 */
#ifndef NDEBUG
	/* This section of code's only "output" is via assert() statements. */
	if (rc == SQL_OK) {
		u64 x = (((u64) 1) << 63) - 1;
		double y;
		assert(sizeof(x) == 8);
		assert(sizeof(x) == sizeof(y));
		memcpy(&y, &x, 8);
		assert(sqlIsNaN(y));
	}
#endif

	return rc;
}

void
sql_row_count(struct sql_context *context, MAYBE_UNUSED int unused1,
	      MAYBE_UNUSED sql_value **unused2)
{
	sql *db = sql_context_db_handle(context);
	sql_result_int(context, db->nChange);
}

/*
 * Close all open savepoints.
 * This procedure is trivial as savepoints are allocated on the "region" and
 * would be destroyed automatically.
 */
void
sqlCloseSavepoints(Vdbe * pVdbe)
{
	pVdbe->anonymous_savepoint = NULL;
}

/*
 * Invoke the destructor function associated with FuncDef p, if any. Except,
 * if this is not the last copy of the function, do not invoke it. Multiple
 * copies of a single function are created when create_function() is called
 * with SQL_ANY as the encoding.
 */
static void
functionDestroy(sql * db, FuncDef * p)
{
	FuncDestructor *pDestructor = p->u.pDestructor;
	if (pDestructor) {
		pDestructor->nRef--;
		if (pDestructor->nRef == 0) {
			pDestructor->xDestroy(pDestructor->pUserData);
			sqlDbFree(db, pDestructor);
		}
	}
}

/*
 * Rollback all database files.  If tripCode is not SQL_OK, then
 * any write cursors are invalidated ("tripped" - as in "tripping a circuit
 * breaker") and made to return tripCode if there are any further
 * attempts to use that cursor.  Read cursors remain open and valid
 * but are "saved" in case the table pages are moved around.
 */
void
sqlRollbackAll(Vdbe * pVdbe)
{
	sql *db = pVdbe->db;

	/* If one has been configured, invoke the rollback-hook callback */
	if (db->xRollbackCallback && (!pVdbe->auto_commit)) {
		db->xRollbackCallback(db->pRollbackArg);
	}
}

/*
 * This function is exactly the same as sql_create_function(), except
 * that it is designed to be called by internal code. The difference is
 * that if a malloc() fails in sql_create_function(), an error code
 * is returned and the mallocFailed flag cleared.
 */
int
sqlCreateFunc(sql * db,
		  const char *zFunctionName,
		  enum field_type type,
		  int nArg,
		  int flags,
		  void *pUserData,
		  void (*xSFunc) (sql_context *, int, sql_value **),
		  void (*xStep) (sql_context *, int, sql_value **),
		  void (*xFinal) (sql_context *),
		  FuncDestructor * pDestructor)
{
	FuncDef *p;
	int extraFlags;

	if (zFunctionName == 0 ||
	    (xSFunc && (xFinal || xStep)) ||
	    (!xSFunc && (xFinal && !xStep)) ||
	    (!xSFunc && (!xFinal && xStep)) ||
	    (nArg < -1 || nArg > SQL_MAX_FUNCTION_ARG) ||
	    (255 < (sqlStrlen30(zFunctionName)))) {
		diag_set(ClientError, ER_CREATE_FUNCTION, zFunctionName,
			 "wrong function definition");
		return SQL_TARANTOOL_ERROR;
	}

	assert(SQL_FUNC_CONSTANT == SQL_DETERMINISTIC);
	extraFlags = flags & SQL_DETERMINISTIC;


	/* Check if an existing function is being overridden or deleted. If so,
	 * and there are active VMs, then return SQL_BUSY. If a function
	 * is being overridden/deleted but there are no active VMs, allow the
	 * operation to continue but invalidate all precompiled statements.
	 */
	p = sqlFindFunction(db, zFunctionName, nArg, 0);
	if (p && p->nArg == nArg) {
		if (db->nVdbeActive) {
			diag_set(ClientError, ER_CREATE_FUNCTION, zFunctionName,
				 "unable to create function due to active "\
				 "statements");
			return SQL_TARANTOOL_ERROR;
		} else {
			sqlExpirePreparedStatements(db);
		}
	}

	p = sqlFindFunction(db, zFunctionName, nArg, 1);
	assert(p || db->mallocFailed);
	if (p == NULL)
		return SQL_TARANTOOL_ERROR;

	/* If an older version of the function with a configured destructor is
	 * being replaced invoke the destructor function here.
	 */
	functionDestroy(db, p);

	if (pDestructor) {
		pDestructor->nRef++;
	}
	p->u.pDestructor = pDestructor;
	p->funcFlags = extraFlags;
	testcase(p->funcFlags & SQL_DETERMINISTIC);
	p->xSFunc = xSFunc ? xSFunc : xStep;
	p->xFinalize = xFinal;
	p->pUserData = pUserData;
	p->nArg = (u16) nArg;
	p->ret_type = type;
	return SQL_OK;
}

int
sql_create_function_v2(sql * db,
			   const char *zFunc,
			   enum field_type type,
			   int nArg,
			   int flags,
			   void *p,
			   void (*xSFunc) (sql_context *, int,
					   sql_value **),
			   void (*xStep) (sql_context *, int,
					  sql_value **),
			   void (*xFinal) (sql_context *),
			   void (*xDestroy) (void *))
{
	int rc = SQL_ERROR;
	FuncDestructor *pArg = 0;

	if (xDestroy) {
		pArg =
		    (FuncDestructor *) sqlDbMallocZero(db,
							   sizeof
							   (FuncDestructor));
		if (!pArg) {
			xDestroy(p);
			goto out;
		}
		pArg->xDestroy = xDestroy;
		pArg->pUserData = p;
	}
	rc = sqlCreateFunc(db, zFunc, type, nArg, flags, p, xSFunc, xStep,
			       xFinal, pArg);
	if (pArg && pArg->nRef == 0) {
		assert(rc != SQL_OK);
		xDestroy(p);
		sqlDbFree(db, pArg);
	}

 out:
	rc = sqlApiExit(db, rc);
	return rc;
}

#ifndef SQL_OMIT_TRACE
/* Register a trace callback using the version-2 interface.
 */
int
sql_trace_v2(sql * db,		/* Trace this connection */
		 unsigned mTrace,	/* Mask of events to be traced */
		 int (*xTrace) (unsigned, void *, void *, void *),	/* Callback to invoke */
		 void *pArg)		/* Context */
{
	if (mTrace == 0)
		xTrace = 0;
	if (xTrace == 0)
		mTrace = 0;
	db->mTrace = mTrace;
	db->xTrace = xTrace;
	db->pTraceArg = pArg;
	return SQL_OK;
}

#endif				/* SQL_OMIT_TRACE */

/*
 * This function returns true if main-memory should be used instead of
 * a temporary file for transient pager files and statement journals.
 * The value returned depends on the value of db->temp_store (runtime
 * parameter) and the compile time value of SQL_TEMP_STORE. The
 * following table describes the relationship between these two values
 * and this functions return value.
 *
 *   SQL_TEMP_STORE     db->temp_store     Location of temporary database
 *   -----------------     --------------     ------------------------------
 *   0                     any                file      (return 0)
 *   1                     1                  file      (return 0)
 *   1                     2                  memory    (return 1)
 *   1                     0                  file      (return 0)
 *   2                     1                  file      (return 0)
 *   2                     2                  memory    (return 1)
 *   2                     0                  memory    (return 1)
 *   3                     any                memory    (return 1)
 */
int
sqlTempInMemory(const sql * db)
{
#if SQL_TEMP_STORE==1
	return (db->temp_store == 2);
#endif
#if SQL_TEMP_STORE==2
	return (db->temp_store != 1);
#endif
#if SQL_TEMP_STORE==3
	UNUSED_PARAMETER(db);
	return 1;
#endif
#if SQL_TEMP_STORE<1 || SQL_TEMP_STORE>3
	UNUSED_PARAMETER(db);
	return 0;
#endif
}

/*
 * This array defines hard upper bounds on limit values.  The
 * initializer must be kept in sync with the SQL_LIMIT_*
 * #defines in sql.h.
 */
static const int aHardLimit[] = {
	SQL_MAX_LENGTH,
	SQL_MAX_SQL_LENGTH,
	SQL_MAX_COLUMN,
	SQL_MAX_EXPR_DEPTH,
	SQL_MAX_COMPOUND_SELECT,
	SQL_MAX_VDBE_OP,
	SQL_MAX_FUNCTION_ARG,
	SQL_MAX_ATTACHED,
	SQL_MAX_LIKE_PATTERN_LENGTH,
	SQL_MAX_TRIGGER_DEPTH,
	SQL_MAX_WORKER_THREADS,
};

/*
 * Make sure the hard limits are set to reasonable values
 */
#if SQL_MAX_LENGTH<100
#error SQL_MAX_LENGTH must be at least 100
#endif
#if SQL_MAX_SQL_LENGTH<100
#error SQL_MAX_SQL_LENGTH must be at least 100
#endif
#if SQL_MAX_SQL_LENGTH>SQL_MAX_LENGTH
#error SQL_MAX_SQL_LENGTH must not be greater than SQL_MAX_LENGTH
#endif
#if SQL_MAX_COMPOUND_SELECT<2
#error SQL_MAX_COMPOUND_SELECT must be at least 2
#endif
#if SQL_MAX_VDBE_OP<40
#error SQL_MAX_VDBE_OP must be at least 40
#endif
#if SQL_MAX_FUNCTION_ARG<0 || SQL_MAX_FUNCTION_ARG>127
#error SQL_MAX_FUNCTION_ARG must be between 0 and 127
#endif
#if SQL_MAX_ATTACHED<0 || SQL_MAX_ATTACHED>125
#error SQL_MAX_ATTACHED must be between 0 and 125
#endif
#if SQL_MAX_LIKE_PATTERN_LENGTH<1
#error SQL_MAX_LIKE_PATTERN_LENGTH must be at least 1
#endif
#if SQL_MAX_COLUMN>32767
#error SQL_MAX_COLUMN must not exceed 32767
#endif
#if SQL_MAX_TRIGGER_DEPTH<1
#error SQL_MAX_TRIGGER_DEPTH must be at least 1
#endif
#if SQL_MAX_WORKER_THREADS<0 || SQL_MAX_WORKER_THREADS>50
#error SQL_MAX_WORKER_THREADS must be between 0 and 50
#endif

/*
 * Change the value of a limit.  Report the old value.
 * If an invalid limit index is supplied, report -1.
 * Make no changes but still report the old value if the
 * new limit is negative.
 *
 * A new lower limit does not shrink existing constructs.
 * It merely prevents new constructs that exceed the limit
 * from forming.
 */
int
sql_limit(sql * db, int limitId, int newLimit)
{
	int oldLimit;

	/* EVIDENCE-OF: R-30189-54097 For each limit category SQL_LIMIT_NAME
	 * there is a hard upper bound set at compile-time by a C preprocessor
	 * macro called SQL_MAX_NAME. (The "_LIMIT_" in the name is changed to
	 * "_MAX_".)
	 */
	assert(aHardLimit[SQL_LIMIT_LENGTH] == SQL_MAX_LENGTH);
	assert(aHardLimit[SQL_LIMIT_SQL_LENGTH] == SQL_MAX_SQL_LENGTH);
	assert(aHardLimit[SQL_LIMIT_COLUMN] == SQL_MAX_COLUMN);
	assert(aHardLimit[SQL_LIMIT_EXPR_DEPTH] == SQL_MAX_EXPR_DEPTH);
	assert(aHardLimit[SQL_LIMIT_COMPOUND_SELECT] ==
	       SQL_MAX_COMPOUND_SELECT);
	assert(aHardLimit[SQL_LIMIT_VDBE_OP] == SQL_MAX_VDBE_OP);
	assert(aHardLimit[SQL_LIMIT_FUNCTION_ARG] ==
	       SQL_MAX_FUNCTION_ARG);
	assert(aHardLimit[SQL_LIMIT_ATTACHED] == SQL_MAX_ATTACHED);
	assert(aHardLimit[SQL_LIMIT_LIKE_PATTERN_LENGTH] ==
	       SQL_MAX_LIKE_PATTERN_LENGTH);
	assert(aHardLimit[SQL_LIMIT_TRIGGER_DEPTH] ==
	       SQL_MAX_TRIGGER_DEPTH);
	assert(aHardLimit[SQL_LIMIT_WORKER_THREADS] ==
	       SQL_MAX_WORKER_THREADS);
	assert(SQL_LIMIT_WORKER_THREADS == (SQL_N_LIMIT - 1));

	if (limitId < 0 || limitId >= SQL_N_LIMIT) {
		return -1;
	}
	oldLimit = db->aLimit[limitId];
	if (newLimit >= 0) {	/* IMP: R-52476-28732 */
		if (newLimit > aHardLimit[limitId]) {
			newLimit = aHardLimit[limitId];	/* IMP: R-51463-25634 */
		}
		db->aLimit[limitId] = newLimit;
	}
	return oldLimit;	/* IMP: R-53341-35419 */
}

/**
 * This routine does the work of initialization of main
 * SQL connection instance.
 *
 * @param[out] out_db returned database handle.
 * @return error status code.
 */
int
sql_init_db(sql **out_db)
{
	sql *db;
	int rc;			/* Return code */

	rc = sql_initialize();
	if (rc)
		return rc;

	/* Allocate the sql data structure */
	db = sqlMallocZero(sizeof(sql));
	if (db == 0)
		goto opendb_out;
	db->errMask = 0xff;
	db->magic = SQL_MAGIC_BUSY;

	db->pVfs = sql_vfs_find(0);

	assert(sizeof(db->aLimit) == sizeof(aHardLimit));
	memcpy(db->aLimit, aHardLimit, sizeof(db->aLimit));
	db->aLimit[SQL_LIMIT_WORKER_THREADS] = SQL_DEFAULT_WORKER_THREADS;
	db->aLimit[SQL_LIMIT_COMPOUND_SELECT] = SQL_DEFAULT_COMPOUND_SELECT;
	db->szMmap = sqlGlobalConfig.szMmap;
	db->nMaxSorterMmap = 0x7FFFFFFF;

	db->magic = SQL_MAGIC_OPEN;
	if (db->mallocFailed) {
		goto opendb_out;
	}

	/* Register all built-in functions, but do not attempt to read the
	 * database schema yet. This is delayed until the first time the database
	 * is accessed.
	 */
	sqlRegisterPerConnectionBuiltinFunctions(db);

opendb_out:
	assert(db != 0 || rc == SQL_NOMEM);
	if (rc == SQL_NOMEM)
		db = NULL;
	else if (rc != SQL_OK)
		db->magic = SQL_MAGIC_SICK;

	*out_db = db;

	return rc;
}

/*
 * This is a utility routine, useful to VFS implementations, that checks
 * to see if a database file was a URI that contained a specific query
 * parameter, and if so obtains the value of the query parameter.
 *
 * The zFilename argument is the filename pointer passed into the xOpen()
 * method of a VFS implementation.  The zParam argument is the name of the
 * query parameter we seek.  This routine returns the value of the zParam
 * parameter if it exists.  If the parameter does not exist, this routine
 * returns a NULL pointer.
 */
const char *
sql_uri_parameter(const char *zFilename, const char *zParam)
{
	if (zFilename == 0 || zParam == 0)
		return 0;
	zFilename += sqlStrlen30(zFilename) + 1;
	while (zFilename[0]) {
		int x = strcmp(zFilename, zParam);
		zFilename += sqlStrlen30(zFilename) + 1;
		if (x == 0)
			return zFilename;
		zFilename += sqlStrlen30(zFilename) + 1;
	}
	return 0;
}
