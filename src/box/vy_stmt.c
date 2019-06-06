/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "vy_stmt.h"

#include <stdlib.h>
#include <string.h>
#include <sys/uio.h> /* struct iovec */
#include <pmatomic.h> /* for refs */

#include "diag.h"
#include <small/region.h>
#include <small/lsregion.h>

#include "error.h"
#include "tuple_bloom.h"
#include "tuple_format.h"
#include "xrow.h"
#include "fiber.h"

/**
 * Statement metadata keys.
 */
enum vy_stmt_meta_key {
	/** Statement flags. */
	VY_STMT_FLAGS = 0x01,
};

/**
 * Return flags that must be persisted when the given statement
 * is written to disk.
 */
static inline uint8_t
vy_stmt_persistent_flags(struct tuple *stmt, bool is_primary)
{
	uint8_t mask = VY_STMT_FLAGS_ALL;

	/*
	 * This flag is only used by the write iterator to turn
	 * in-memory REPLACEs into INSERTs on dump so no need to
	 * persist it.
	 */
	mask &= ~VY_STMT_UPDATE;

	if (!is_primary) {
		/*
		 * Do not store VY_STMT_DEFERRED_DELETE flag in
		 * secondary index runs as deferred DELETEs may
		 * only be generated by primary index compaction.
		 */
		mask &= ~VY_STMT_DEFERRED_DELETE;
	}
	return vy_stmt_flags(stmt) & mask;
}

static struct tuple *
vy_tuple_new(struct tuple_format *format, const char *data, const char *end)
{
	if (tuple_validate_raw(format, data) != 0)
		return NULL;

	struct tuple *tuple = vy_stmt_new_insert(format, data, end);
	if (tuple != NULL) {
		tuple_bless(tuple);
		tuple_unref(tuple);
	}
	return tuple;
}

static void
vy_tuple_delete(struct tuple_format *format, struct tuple *tuple)
{
	say_debug("%s(%p)", __func__, tuple);
	assert(tuple->refs == 0);
	/*
	 * Turn off formats referencing in worker threads to avoid
	 * multithread unsafe modifications of a reference
	 * counter.
	 */
	if (cord_is_main())
		tuple_format_unref(format);
#ifndef NDEBUG
	memset(tuple, '#', tuple_size(tuple)); /* fail early */
#endif
	free(tuple);
}

void
vy_stmt_env_create(struct vy_stmt_env *env)
{
	env->tuple_format_vtab.tuple_new = vy_tuple_new;
	env->tuple_format_vtab.tuple_delete = vy_tuple_delete;
	env->tuple_format_vtab.tuple_extra_new = NULL;
	env->tuple_format_vtab.tuple_extra_delete = NULL;
	env->tuple_format_vtab.tuple_extra_get = NULL;
	env->max_tuple_size = 1024 * 1024;
	env->key_format = vy_stmt_format_new(env, NULL, 0, NULL, 0, 0, NULL);
	if (env->key_format == NULL)
		panic("failed to create vinyl key format");
	tuple_format_ref(env->key_format);
}

void
vy_stmt_env_destroy(struct vy_stmt_env *env)
{
	tuple_format_unref(env->key_format);
}

struct tuple_format *
vy_stmt_format_new(struct vy_stmt_env *env, struct key_def *const *keys,
		   uint16_t key_count, const struct field_def *fields,
		   uint32_t field_count, uint32_t exact_field_count,
		   struct tuple_dictionary *dict)
{
	return tuple_format_new(&env->tuple_format_vtab, env, keys, key_count,
				fields, field_count, exact_field_count, dict,
				false, false);
}

/**
 * Allocate a vinyl statement object on base of the struct tuple
 * with malloc() and the reference counter equal to 1.
 * @param format Format of an index.
 * @param data_offset Offset of MessagePack data within the tuple.
 * @param bsize  Size of the variable part of the statement. It
 *               includes size of MessagePack tuple data and, for
 *               upserts, MessagePack array of operations.
 * @retval not NULL Success.
 * @retval     NULL Memory error.
 */
static struct tuple *
vy_stmt_alloc(struct tuple_format *format, uint32_t data_offset, uint32_t bsize)
{
	assert(data_offset >= sizeof(struct vy_stmt) + format->field_map_size);
	struct vy_stmt_env *env = format->engine;
	uint32_t total_size = data_offset + bsize;
	if (unlikely(total_size > env->max_tuple_size)) {
		diag_set(ClientError, ER_VINYL_MAX_TUPLE_SIZE,
			 (unsigned) total_size);
		error_log(diag_last_error(diag_get()));
		return NULL;
	}
	struct tuple *tuple = malloc(total_size);
	if (unlikely(tuple == NULL)) {
		diag_set(OutOfMemory, total_size, "malloc", "struct vy_stmt");
		return NULL;
	}
	say_debug("vy_stmt_alloc(format = %d data_offset = %u, bsize = %u) = %p",
		  format->id, data_offset, bsize, tuple);
	tuple->refs = 1;
	tuple->format_id = tuple_format_id(format);
	if (cord_is_main())
		tuple_format_ref(format);
	tuple->bsize = bsize;
	tuple->data_offset = data_offset;
	vy_stmt_set_lsn(tuple, 0);
	vy_stmt_set_type(tuple, 0);
	vy_stmt_set_flags(tuple, 0);
	return tuple;
}

struct tuple *
vy_stmt_dup(struct tuple *stmt)
{
	/*
	 * We don't use tuple_new() to avoid the initializing of
	 * tuple field map. This map can be simple memcopied from
	 * the original tuple.
	 */
	struct tuple *res = vy_stmt_alloc(tuple_format(stmt),
					  stmt->data_offset, stmt->bsize);
	if (res == NULL)
		return NULL;
	assert(tuple_size(res) == tuple_size(stmt));
	assert(res->data_offset == stmt->data_offset);
	memcpy(res, stmt, tuple_size(stmt));
	res->refs = 1;
	return res;
}

struct tuple *
vy_stmt_dup_lsregion(struct tuple *stmt, struct lsregion *lsregion,
		     int64_t alloc_id)
{
	enum iproto_type type = vy_stmt_type(stmt);
	size_t size = tuple_size(stmt);
	size_t alloc_size = size;
	struct tuple *mem_stmt;

	/* Reserve one byte for UPSERT counter. */
	if (type == IPROTO_UPSERT)
		alloc_size++;

	mem_stmt = lsregion_alloc(lsregion, alloc_size, alloc_id);
	if (mem_stmt == NULL) {
		diag_set(OutOfMemory, size, "lsregion_alloc", "mem_stmt");
		return NULL;
	}

	if (type == IPROTO_UPSERT) {
		*(uint8_t *)mem_stmt = 0;
		mem_stmt = (struct tuple *)((uint8_t *)mem_stmt + 1);
	}

	memcpy(mem_stmt, stmt, size);
	/*
	 * Region allocated statements can't be referenced or unreferenced
	 * because they are located in monolithic memory region. Referencing has
	 * sense only for separately allocated memory blocks.
	 * The reference count here is set to 0 for an assertion if somebody
	 * will try to unreference this statement.
	 */
	mem_stmt->refs = 0;
	return mem_stmt;
}

struct tuple *
vy_key_new(struct tuple_format *format, const char *key, uint32_t part_count)
{
	assert(vy_stmt_is_key_format(format));
	assert(part_count == 0 || key != NULL);
	/* Key don't have field map */
	assert(format->field_map_size == 0);

	/* Calculate key length */
	const char *key_end = key;
	for (uint32_t i = 0; i < part_count; i++)
		mp_next(&key_end);

	/* Allocate stmt */
	uint32_t key_size = key_end - key;
	uint32_t bsize = mp_sizeof_array(part_count) + key_size;
	struct tuple *stmt = vy_stmt_alloc(format, sizeof(struct vy_stmt), bsize);
	if (stmt == NULL)
		return NULL;
	/* Copy MsgPack data */
	char *raw = (char *) stmt + sizeof(struct vy_stmt);
	char *data = mp_encode_array(raw, part_count);
	memcpy(data, key, key_size);
	assert(data + key_size == raw + bsize);
	return stmt;
}

char *
vy_key_dup(const char *key)
{
	assert(mp_typeof(*key) == MP_ARRAY);
	const char *end = key;
	mp_next(&end);
	char *res = malloc(end - key);
	if (res == NULL) {
		diag_set(OutOfMemory, end - key, "malloc", "key");
		return NULL;
	}
	memcpy(res, key, end - key);
	return res;
}

/**
 * Create a statement without type and with reserved space for operations.
 * Operations can be saved in the space available by @param extra.
 * For details @sa struct vy_stmt comment.
 */
static struct tuple *
vy_stmt_new_with_ops(struct tuple_format *format, const char *tuple_begin,
		     const char *tuple_end, struct iovec *ops,
		     int op_count, enum iproto_type type)
{
	mp_tuple_assert(tuple_begin, tuple_end);

	const char *tmp = tuple_begin;
	mp_decode_array(&tmp);

	size_t ops_size = 0;
	for (int i = 0; i < op_count; ++i)
		ops_size += ops[i].iov_len;

	struct tuple *stmt = NULL;
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	/*
	 * Calculate offsets for key parts.
	 *
	 * Note, an overwritten statement loaded from a primary
	 * index run file may not conform to the current format
	 * in case the space was altered (e.g. a new field was
	 * added which is missing in a deleted tuple). Although
	 * we should never return such statements to the user,
	 * we may still need to decode them while iterating over
	 * a run so we skip tuple validation here. This is OK as
	 * tuples inserted into a space are validated explicitly
	 * with tuple_validate() anyway.
	 */
	struct field_map_builder builder;
	if (tuple_field_map_create(format, tuple_begin, false, &builder) != 0)
		goto end;
	uint32_t field_map_size = field_map_build_size(&builder);
	/*
	 * Allocate stmt. Offsets: one per key part + offset of the
	 * statement end.
	 */
	size_t mpsize = (tuple_end - tuple_begin);
	size_t bsize = mpsize + ops_size;
	stmt = vy_stmt_alloc(format, sizeof(struct vy_stmt) +
			     field_map_size, bsize);
	if (stmt == NULL)
		goto end;
	/* Copy MsgPack data */
	char *raw = (char *) tuple_data(stmt);
	char *wpos = raw;
	field_map_build(&builder, wpos - field_map_size);
	memcpy(wpos, tuple_begin, mpsize);
	wpos += mpsize;
	for (struct iovec *op = ops, *end = ops + op_count;
	     op != end; ++op) {
		memcpy(wpos, op->iov_base, op->iov_len);
		wpos += op->iov_len;
	}
	vy_stmt_set_type(stmt, type);
end:
	region_truncate(region, region_svp);
	return stmt;
}

struct tuple *
vy_stmt_new_upsert(struct tuple_format *format, const char *tuple_begin,
		   const char *tuple_end, struct iovec *operations,
		   uint32_t ops_cnt)
{
	return vy_stmt_new_with_ops(format, tuple_begin, tuple_end,
				    operations, ops_cnt, IPROTO_UPSERT);
}

struct tuple *
vy_stmt_new_replace(struct tuple_format *format, const char *tuple_begin,
		    const char *tuple_end)
{
	return vy_stmt_new_with_ops(format, tuple_begin, tuple_end,
				    NULL, 0, IPROTO_REPLACE);
}

struct tuple *
vy_stmt_new_insert(struct tuple_format *format, const char *tuple_begin,
		   const char *tuple_end)
{
	return vy_stmt_new_with_ops(format, tuple_begin, tuple_end,
				    NULL, 0, IPROTO_INSERT);
}

struct tuple *
vy_stmt_new_delete(struct tuple_format *format, const char *tuple_begin,
		   const char *tuple_end)
{
	return vy_stmt_new_with_ops(format, tuple_begin, tuple_end,
				    NULL, 0, IPROTO_DELETE);
}

struct tuple *
vy_stmt_replace_from_upsert(struct tuple *upsert)
{
	assert(vy_stmt_type(upsert) == IPROTO_UPSERT);
	/* Get statement size without UPSERT operations */
	uint32_t bsize;
	vy_upsert_data_range(upsert, &bsize);
	assert(bsize <= upsert->bsize);

	/* Copy statement data excluding UPSERT operations */
	struct tuple_format *format = tuple_format(upsert);
	struct tuple *replace = vy_stmt_alloc(format, upsert->data_offset, bsize);
	if (replace == NULL)
		return NULL;
	/* Copy both data and field_map. */
	char *dst = (char *)replace + sizeof(struct vy_stmt);
	char *src = (char *)upsert + sizeof(struct vy_stmt);
	memcpy(dst, src, upsert->data_offset + bsize - sizeof(struct vy_stmt));
	vy_stmt_set_type(replace, IPROTO_REPLACE);
	vy_stmt_set_lsn(replace, vy_stmt_lsn(upsert));
	return replace;
}

struct tuple *
vy_stmt_new_surrogate_delete_raw(struct tuple_format *format,
				 const char *src_data, const char *src_data_end)
{
	struct tuple *stmt = NULL;
	uint32_t src_size = src_data_end - src_data;
	/* Surrogate tuple uses less memory than the original tuple */
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	char *data = region_alloc(region, src_size);
	if (data == NULL) {
		diag_set(OutOfMemory, src_size, "region", "tuple");
		return NULL;
	}
	struct field_map_builder builder;
	if (field_map_builder_create(&builder, format->field_map_size,
				     region) != 0)
		goto out;
	/*
	 * Perform simultaneous parsing of the tuple and
	 * format::fields tree traversal to copy indexed field
	 * data and initialize field map.
	 */
	uint32_t field_count;
	struct tuple_format_iterator it;
	if (tuple_format_iterator_create(&it, format, src_data,
			TUPLE_FORMAT_ITERATOR_KEY_PARTS_ONLY, &field_count,
			region) != 0)
		goto out;
	char *pos = mp_encode_array(data, field_count);
	struct tuple_format_iterator_entry entry;
	while (tuple_format_iterator_next(&it, &entry) == 0 &&
	       entry.data != NULL) {
		if (entry.field == NULL) {
			/*
			 * Instead of copying useless data having
			 * no representation in tuple_format,
			 * write nil.
			 */
			pos = mp_encode_nil(pos);
			if (entry.parent != NULL &&
			    entry.parent->type == FIELD_TYPE_MAP)
				pos = mp_encode_nil(pos);
			continue;
		}
		if (entry.field->token.type == JSON_TOKEN_STR) {
			pos = mp_encode_str(pos, entry.field->token.str,
					    entry.field->token.len);
		}
		/* Initialize field_map with data offset. */
		uint32_t offset_slot = entry.field->offset_slot;
		if (offset_slot != TUPLE_OFFSET_SLOT_NIL &&
		    field_map_builder_set_slot(&builder, offset_slot,
					pos - data, entry.multikey_idx,
					entry.multikey_count, region) != 0)
			goto out;
		/* Copy field data. */
		if (entry.field->type == FIELD_TYPE_ARRAY) {
			pos = mp_encode_array(pos, entry.count);
		} else if (entry.field->type == FIELD_TYPE_MAP) {
			pos = mp_encode_map(pos, entry.count);
		} else {
			memcpy(pos, entry.data, entry.data_end - entry.data);
			pos += entry.data_end - entry.data;
		}
	}
	if (entry.data != NULL)
		goto out;
	assert(pos <= data + src_size);
	uint32_t bsize = pos - data;
	uint32_t field_map_size = field_map_build_size(&builder);
	stmt = vy_stmt_alloc(format, sizeof(struct vy_stmt) + field_map_size,
			     bsize);
	if (stmt == NULL)
		goto out;
	char *stmt_data = (char *) tuple_data(stmt);
	char *stmt_field_map_begin = stmt_data - field_map_size;
	memcpy(stmt_data, data, bsize);
	field_map_build(&builder, stmt_field_map_begin);
	vy_stmt_set_type(stmt, IPROTO_DELETE);
	mp_tuple_assert(stmt_data, stmt_data + bsize);
out:
	region_truncate(region, region_svp);
	return stmt;
}

struct tuple *
vy_stmt_extract_key(struct tuple *stmt, struct key_def *key_def,
		    struct tuple_format *format, int multikey_idx)
{
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	const char *key_raw = tuple_extract_key(stmt, key_def,
						multikey_idx, NULL);
	if (key_raw == NULL)
		return NULL;
	uint32_t part_count = mp_decode_array(&key_raw);
	assert(part_count == key_def->part_count);
	struct tuple *key = vy_key_new(format, key_raw, part_count);
	/* Cleanup memory allocated by tuple_extract_key(). */
	region_truncate(region, region_svp);
	return key;
}

struct tuple *
vy_stmt_extract_key_raw(const char *data, const char *data_end,
			struct key_def *key_def, struct tuple_format *format,
			int multikey_idx)
{
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	const char *key_raw = tuple_extract_key_raw(data, data_end, key_def,
						    multikey_idx, NULL);
	if (key_raw == NULL)
		return NULL;
	uint32_t part_count = mp_decode_array(&key_raw);
	assert(part_count == key_def->part_count);
	struct tuple *key = vy_key_new(format, key_raw, part_count);
	/* Cleanup memory allocated by tuple_extract_key_raw(). */
	region_truncate(region, region_svp);
	return key;
}

int
vy_bloom_builder_add(struct tuple_bloom_builder *builder,
		     struct vy_entry entry, struct key_def *key_def)
{
	struct tuple *stmt = entry.stmt;
	if (vy_stmt_is_key(stmt)) {
		const char *data = tuple_data(stmt);
		uint32_t part_count = mp_decode_array(&data);
		return tuple_bloom_builder_add_key(builder, data,
						   part_count, key_def);
	} else {
		return tuple_bloom_builder_add(builder, stmt, key_def,
				vy_entry_multikey_idx(entry, key_def));
	}
}

bool
vy_bloom_maybe_has(const struct tuple_bloom *bloom,
		   struct vy_entry entry, struct key_def *key_def)
{
	struct tuple *stmt = entry.stmt;
	if (vy_stmt_is_key(stmt)) {
		const char *data = tuple_data(stmt);
		uint32_t part_count = mp_decode_array(&data);
		return tuple_bloom_maybe_has_key(bloom, data,
						 part_count, key_def);
	} else {
		return tuple_bloom_maybe_has(bloom, stmt, key_def,
				vy_entry_multikey_idx(entry, key_def));
	}
}

/**
 * Encode the given statement meta data in a request.
 * Returns 0 on success, -1 on memory allocation error.
 */
static int
vy_stmt_meta_encode(struct tuple *stmt, struct request *request,
		    bool is_primary)
{
	uint8_t flags = vy_stmt_persistent_flags(stmt, is_primary);
	if (flags == 0)
		return 0; /* nothing to encode */

	size_t len = mp_sizeof_map(1) * 2 * mp_sizeof_uint(UINT64_MAX);
	char *buf = region_alloc(&fiber()->gc, len);
	if (buf == NULL)
		return -1;
	char *pos = buf;
	pos = mp_encode_map(pos, 1);
	pos = mp_encode_uint(pos, VY_STMT_FLAGS);
	pos = mp_encode_uint(pos, flags);
	assert(pos <= buf + len);

	request->tuple_meta = buf;
	request->tuple_meta_end = pos;
	return 0;
}

/**
 * Decode statement meta data from a request.
 */
static void
vy_stmt_meta_decode(struct request *request, struct tuple *stmt)
{
	const char *data = request->tuple_meta;
	if (data == NULL)
		return; /* nothing to decode */

	uint32_t size = mp_decode_map(&data);
	for (uint32_t i = 0; i < size; i++) {
		uint64_t key = mp_decode_uint(&data);
		switch (key) {
		case VY_STMT_FLAGS: {
			uint64_t flags = mp_decode_uint(&data);
			vy_stmt_set_flags(stmt, flags);
			break;
		}
		default:
			mp_next(&data); /* unknown key, ignore */
		}
	}
}

int
vy_stmt_encode_primary(struct tuple *value, struct key_def *key_def,
		       uint32_t space_id, struct xrow_header *xrow)
{
	memset(xrow, 0, sizeof(*xrow));
	enum iproto_type type = vy_stmt_type(value);
	xrow->type = type;
	xrow->lsn = vy_stmt_lsn(value);

	struct request request;
	memset(&request, 0, sizeof(request));
	request.type = type;
	request.space_id = space_id;
	uint32_t size;
	const char *extracted = NULL;
	switch (type) {
	case IPROTO_DELETE:
		extracted = vy_stmt_is_key(value) ?
			    tuple_data_range(value, &size) :
			    tuple_extract_key(value, key_def,
					      MULTIKEY_NONE, &size);
		if (extracted == NULL)
			return -1;
		request.key = extracted;
		request.key_end = request.key + size;
		break;
	case IPROTO_INSERT:
	case IPROTO_REPLACE:
		request.tuple = tuple_data_range(value, &size);
		request.tuple_end = request.tuple + size;
		break;
	case IPROTO_UPSERT:
		request.tuple = vy_upsert_data_range(value, &size);
		request.tuple_end = request.tuple + size;
		/* extract operations */
		request.ops = vy_stmt_upsert_ops(value, &size);
		request.ops_end = request.ops + size;
		break;
	default:
		unreachable();
	}
	if (vy_stmt_meta_encode(value, &request, true) != 0)
		return -1;
	xrow->bodycnt = xrow_encode_dml(&request, &fiber()->gc, xrow->body);
	if (xrow->bodycnt < 0)
		return -1;
	return 0;
}

int
vy_stmt_encode_secondary(struct tuple *value, struct key_def *cmp_def,
			 int multikey_idx, struct xrow_header *xrow)
{
	memset(xrow, 0, sizeof(*xrow));
	enum iproto_type type = vy_stmt_type(value);
	xrow->type = type;
	xrow->lsn = vy_stmt_lsn(value);

	struct request request;
	memset(&request, 0, sizeof(request));
	request.type = type;
	uint32_t size;
	const char *extracted = vy_stmt_is_key(value) ?
				tuple_data_range(value, &size) :
				tuple_extract_key(value, cmp_def,
						  multikey_idx, &size);
	if (extracted == NULL)
		return -1;
	if (type == IPROTO_REPLACE || type == IPROTO_INSERT) {
		request.tuple = extracted;
		request.tuple_end = extracted + size;
	} else {
		assert(type == IPROTO_DELETE);
		request.key = extracted;
		request.key_end = extracted + size;
	}
	if (vy_stmt_meta_encode(value, &request, false) != 0)
		return -1;
	xrow->bodycnt = xrow_encode_dml(&request, &fiber()->gc, xrow->body);
	if (xrow->bodycnt < 0)
		return -1;
	else
		return 0;
}

struct tuple *
vy_stmt_decode(struct xrow_header *xrow, struct tuple_format *format)
{
	struct vy_stmt_env *env = format->engine;
	struct request request;
	uint64_t key_map = dml_request_key_map(xrow->type);
	key_map &= ~(1ULL << IPROTO_SPACE_ID); /* space_id is optional */
	if (xrow_decode_dml(xrow, &request, key_map) != 0)
		return NULL;
	struct tuple *stmt = NULL;
	struct iovec ops;
	switch (request.type) {
	case IPROTO_DELETE:
		/* Always use key format for DELETE statements. */
		stmt = vy_stmt_new_with_ops(env->key_format,
					    request.key, request.key_end,
					    NULL, 0, IPROTO_DELETE);
		break;
	case IPROTO_INSERT:
	case IPROTO_REPLACE:
		stmt = vy_stmt_new_with_ops(format, request.tuple,
					    request.tuple_end,
					    NULL, 0, request.type);
		break;
	case IPROTO_UPSERT:
		ops.iov_base = (char *)request.ops;
		ops.iov_len = request.ops_end - request.ops;
		stmt = vy_stmt_new_upsert(format, request.tuple,
					  request.tuple_end, &ops, 1);
		break;
	default:
		/* TODO: report filename. */
		diag_set(ClientError, ER_INVALID_RUN_FILE,
			 tt_sprintf("Can't decode statement: "
				    "unknown request type %u",
				    (unsigned)request.type));
		return NULL;
	}

	if (stmt == NULL)
		return NULL; /* OOM */

	vy_stmt_meta_decode(&request, stmt);
	vy_stmt_set_lsn(stmt, xrow->lsn);
	return stmt;
}

int
vy_stmt_snprint(char *buf, int size, struct tuple *stmt)
{
	int total = 0;
	uint32_t mp_size;
	if (stmt == NULL) {
		SNPRINT(total, snprintf, buf, size, "<NULL>");
		return total;
	}
	if (vy_stmt_type(stmt) == 0) {
		SNPRINT(total, mp_snprint, buf, size, tuple_data(stmt));
		return total;
	}
	SNPRINT(total, snprintf, buf, size, "%s(",
		iproto_type_name(vy_stmt_type(stmt)));
		SNPRINT(total, mp_snprint, buf, size, tuple_data(stmt));
	if (vy_stmt_type(stmt) == IPROTO_UPSERT) {
		SNPRINT(total, snprintf, buf, size, ", ops=");
		SNPRINT(total, mp_snprint, buf, size,
			vy_stmt_upsert_ops(stmt, &mp_size));
	}
	SNPRINT(total, snprintf, buf, size, ", lsn=%lld)",
		(long long) vy_stmt_lsn(stmt));
	return total;
}

const char *
vy_stmt_str(struct tuple *stmt)
{
	char *buf = tt_static_buf();
	if (vy_stmt_snprint(buf, TT_STATIC_BUF_LEN, stmt) < 0)
		return "<failed to format statement>";
	return buf;
}
