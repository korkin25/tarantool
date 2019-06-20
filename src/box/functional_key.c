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
#include "functional_key.h"
#include "index.h"
#include "box.h"
#include "fiber.h"
#include "func.h"
#include "func_cache.h"
#include "port.h"
#include "stdbool.h"
#include "tuple.h"

/**
 * Execute a given functional index extractor function and
 * return an extracted key_data and key_data_sz.
 */
static const char *
functional_key_extract(struct func *func, struct port *in_port,
		       uint32_t *key_data_sz)
{
	struct port out_port;
	int rc = func_call(func, in_port, &out_port);
	if (rc != 0)
		goto error;
	const char *key_data = port_get_msgpack(&out_port, key_data_sz);
	port_destroy(&out_port);
	if (key_data == NULL)
		goto error;

	return key_data;
error:
	diag_set(ClientError, ER_FUNCTIONAL_EXTRACTOR, func->def->name,
		 diag_last_error(diag_get())->errmsg);
	return NULL;
}

/**
 * Process a given data and initialize a key_map allocation.
 * Perform key validation if validate == true.
 */
static int
functional_key_map_create(struct func *func, struct key_def *key_def,
			  const char *data, uint32_t key_count,
			  uint32_t *key_map, bool validate)
{
	const char *key = data;
	for (uint32_t key_idx = 0; key_idx < key_count; key_idx++) {
		if (key_map != NULL)
			key_map[key_idx] = key - data;
		if (validate && mp_typeof(*key) != MP_ARRAY) {
			diag_set(ClientError, ER_FUNCTIONAL_EXTRACTOR,
				 func->def->name,
				 "returned key type is invalid");
			return -1;
		}

		const char *key_end;
		uint32_t part_count = mp_decode_array(&key);
		if (!validate) {
			key_end = key;
			mp_next(&key_end);
		} else if (exact_key_validate(key_def, key, part_count,
					      &key_end) != 0) {
			diag_set(ClientError, ER_FUNCTIONAL_EXTRACTOR,
				 func->def->name,
				 diag_last_error(diag_get())->errmsg);
			return -1;
		}
		key = key_end;
	}
	if (key_map != NULL)
		key_map[0] = key_count;
	return 0;
}

/**
 * Process a given raw functional index key data returned by
 * functional index extractor routine to form a key used in
 * comparators and initialize tuple_extra extention
 * (when enabled) and corresponding key_map.
 * Perform key validation if validate == true.
 */
static const char *
functional_key_prepare(struct func *func, struct key_def *key_def,
		       struct tuple *tuple, const char *key_data,
		       uint32_t key_data_sz, bool validate,
		       uint32_t *key_count, uint32_t **key_map)
{
	*key_count = mp_decode_array(&key_data);
	key_data_sz -= mp_sizeof_array(*key_count);
	if (validate && (!key_def->is_multikey && *key_count > 1)) {
		diag_set(ClientError, ER_FUNCTIONAL_EXTRACTOR,
			 func->def->name, "to many keys were returned");
		return NULL;
	}

#ifndef FUNCTIONAL_KEY_HASH_IS_DISABLED
	uint32_t key_map_sz =
		functional_key_map_sz(*key_count);
	struct tuple_extra *tuple_extra =
		tuple_extra_new(tuple, func->def->fid,
				key_data_sz + key_map_sz);
	if (tuple_extra == NULL)
		return NULL;

	memcpy(tuple_extra->data + key_map_sz, key_data,
		key_data_sz);
	*key_map = (uint32_t *) tuple_extra->data;
	key_data = tuple_extra->data + key_map_sz;
#else
	(void) tuple;
	*key_map = NULL;
#endif /* FUNCTIONAL_KEY_HASH_IS_DISABLED */

	if (functional_key_map_create(func, key_def, key_data, *key_count,
				      *key_map, validate) != 0) {
#ifndef FUNCTIONAL_KEY_HASH_IS_DISABLED
		tuple_extra_delete(tuple_extra);
#endif
		return NULL;
	}
	return key_data;
}

const char *
functional_key_get(struct tuple *tuple, uint32_t functional_fid,
		   uint32_t *key_count, uint32_t **key_map)
{
#ifndef FUNCTIONAL_KEY_HASH_IS_DISABLED
	struct tuple_extra *tuple_extra =
		tuple_extra_get(tuple, functional_fid);
	if (likely(tuple_extra != NULL)) {
		assert(tuple_extra != NULL);
		*key_map = (uint32_t *) tuple_extra->data;
		*key_count = (*key_map)[0];
		return tuple_extra->data + *key_count * sizeof(uint32_t);
	}
#endif /* FUNCTIONAL_KEY_HASH_IS_DISABLED */

	/** Index may be created on space with data. */
	struct func *func = func_by_id(functional_fid);
	assert(func != NULL);

	struct port in_port;
	port_tuple_create(&in_port);
	port_tuple_add(&in_port, tuple);
	uint32_t key_data_sz;
	const char *key_data =
		functional_key_extract(func, &in_port, &key_data_sz);
	port_destroy(&in_port);
	if (key_data == NULL)
		goto error;

	key_data = functional_key_prepare(func, NULL, tuple, key_data,
				key_data_sz, false, key_count, key_map);
	if (key_data == NULL)
		goto error;
	return key_data;
error:
	panic_syserror("Functional index runtime exception: %s",
			diag_last_error(diag_get())->errmsg);
}

int
functional_keys_materialize(struct tuple_format *format, struct tuple *tuple)
{
	assert(!rlist_empty(&format->functional_handle));
	struct region *region = &fiber()->gc;
	uint32_t region_svp = region_used(region);

	struct port in_port;
	port_tuple_create(&in_port);
	port_tuple_add(&in_port, tuple);
	int extent_cnt = 0;
	struct functional_handle *handle;
	rlist_foreach_entry(handle, &format->functional_handle, link) {
		assert(tuple_extra_get(tuple,
				handle->key_def->functional_fid) == NULL);
		if (unlikely(handle->func == NULL)) {
			/**
			 * The functional handle function pointer
			 * initialization had been delayed during
			 * recovery. Initialize it.
			 */
			assert(strcmp(box_status(), "loading") == 0);
			handle->func =
				func_by_id(handle->key_def->functional_fid);
			assert(handle->func != NULL);
			func_ref(handle->func);
		}
		struct key_def *key_def = handle->key_def;
		struct func *func = handle->func;
		uint32_t key_data_sz;
		const char *key_data =
			functional_key_extract(func, &in_port,
					       &key_data_sz);
		if (key_data == NULL)
			goto error;

		uint32_t *key_map, key_count;
		key_data = functional_key_prepare(func, key_def, tuple,
						  key_data, key_data_sz,
						  true, &key_count, &key_map);
		if (key_data == NULL)
			goto error;

		region_truncate(region, region_svp);
		extent_cnt++;
	}
	port_destroy(&in_port);
	region_truncate(region, region_svp);
	return 0;
error:
	port_destroy(&in_port);
	region_truncate(region, region_svp);
#ifndef FUNCTIONAL_KEY_HASH_IS_DISABLED
	rlist_foreach_entry(handle, &format->functional_handle, link) {
		if (extent_cnt-- == 0)
			break;
		struct tuple_extra *tuple_extra =
			tuple_extra_get(tuple, handle->key_def->functional_fid);
		assert(tuple_extra != NULL);
		tuple_extra_delete(tuple_extra);
	}
#endif /* FUNCTIONAL_KEY_HASH_IS_DISABLED */
	return -1;
}

void
functional_keys_terminate(struct tuple_format *format, struct tuple *tuple)
{
	assert(!rlist_empty(&format->functional_handle));

	struct functional_handle *handle;
	rlist_foreach_entry(handle, &format->functional_handle, link) {
		struct tuple_extra *tuple_extra =
			tuple_extra_get(tuple, handle->key_def->functional_fid);
		if (tuple_extra == NULL)
			continue;
		assert(tuple_extra != NULL);
		tuple_extra_delete(tuple_extra);
	}
}
