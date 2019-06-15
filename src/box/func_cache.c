/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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
#include "func_cache.h"
#include "assoc.h"
#include "diag.h"
#include "errcode.h"
#include "error.h"
#include "func.h"
#include "schema_def.h"

/** mhash table (name, len -> collation) */
static struct mh_i32ptr_t *func_id_cache;
/** mhash table (id -> collation) */
static struct mh_strnptr_t *func_name_cache;

int
func_cache_init(void)
{
	func_id_cache = mh_i32ptr_new();
	if (func_id_cache == NULL) {
		diag_set(OutOfMemory, sizeof(*func_id_cache), "malloc",
			 "func_id_cache");
		return -1;
	}
	func_name_cache = mh_strnptr_new();
	if (func_name_cache == NULL) {
		diag_set(OutOfMemory, sizeof(*func_name_cache), "malloc",
			 "func_name_cache");
		mh_i32ptr_delete(func_id_cache);
		return -1;
	}
	return 0;
}

void
func_cache_destroy(void)
{
	mh_strnptr_delete(func_name_cache);
	mh_i32ptr_delete(func_id_cache);
}

void
func_cache_insert(struct func *func)
{
	assert(func_by_id(func->def->fid) == NULL);
	assert(func_by_name(func->def->name, strlen(func->def->name)) == NULL);
	const struct mh_i32ptr_node_t node = { func->def->fid, func };
	mh_int_t k1 = mh_i32ptr_put(func_id_cache, &node, NULL, NULL);
	if (k1 == mh_end(func_id_cache)) {
error:
		panic_syserror("Out of memory for the data "
			       "dictionary cache (stored function).");
	}
	size_t def_name_len = strlen(func->def->name);
	uint32_t name_hash = mh_strn_hash(func->def->name, def_name_len);
	const struct mh_strnptr_node_t strnode = {
		func->def->name, def_name_len, name_hash, func };
	mh_int_t k2 = mh_strnptr_put(func_name_cache, &strnode, NULL, NULL);
	if (k2 == mh_end(func_name_cache)) {
		mh_i32ptr_del(func_id_cache, k1, NULL);
		goto error;
	}
}

void
func_cache_delete(uint32_t fid)
{
	mh_int_t k = mh_i32ptr_find(func_id_cache, fid, NULL);
	if (k == mh_end(func_id_cache))
		return;
	struct func *func = (struct func *)
		mh_i32ptr_node(func_id_cache, k)->val;
	mh_i32ptr_del(func_id_cache, k, NULL);
	k = mh_strnptr_find_inp(func_name_cache, func->def->name,
				strlen(func->def->name));
	if (k != mh_end(func_id_cache))
		mh_strnptr_del(func_name_cache, k, NULL);
}

struct func *
func_by_id(uint32_t fid)
{
	mh_int_t func = mh_i32ptr_find(func_id_cache, fid, NULL);
	if (func == mh_end(func_id_cache))
		return NULL;
	return (struct func *) mh_i32ptr_node(func_id_cache, func)->val;
}

struct func *
func_by_name(const char *name, uint32_t name_len)
{
	mh_int_t func = mh_strnptr_find_inp(func_name_cache, name, name_len);
	if (func == mh_end(func_name_cache))
		return NULL;
	return (struct func *) mh_strnptr_node(func_name_cache, func)->val;
}
