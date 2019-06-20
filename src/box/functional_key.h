#ifndef TARANTOOL_BOX_FUNCTIONAL_KEY_H_INCLUDED
#define TARANTOOL_BOX_FUNCTIONAL_KEY_H_INCLUDED
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
#ifdef __cplusplus
extern "C" {
#endif

#include "inttypes.h"

struct tuple;
struct tuple_format;

/**
 * Functional key map is auxilary memroy allocation having the
 * following layout:
 *
 *       4b          4b         4b           4b
 * +-----------+-----------+-----------+-----------+ +------+----+
 * | key_count |key2_offset|    ...    |keyN_offset| |header|data|
 * +-----------+-----------+-----------+-----------+ +------+----+
 *                                                   | key1
 *
 * The functional key map is a part of tuple_extra allocation
 * representing initialized functional key, when tuple_extra cache
 * is enabled.
 */
static inline uint32_t
functional_key_map_sz(uint32_t key_count)
{
	return sizeof(uint32_t) * key_count;
}

/**
 * Process all functional index handles are associated with given
 * tuple format, evaluate the corresponding extractors with given
 * tuple, validate extracted keys (when validate == true) and
 * register functional keys in tuple_extra cache (when enabled).
 */
int
functional_keys_materialize(struct tuple_format *format, struct tuple *tuple);

/** Terminate all registered functional index keys. */
void
functional_keys_terminate(struct tuple_format *format, struct tuple *tuple);

/**
 * Get functional index key by given tuple and function
 * identifier.
 */
const char *
functional_key_get(struct tuple *tuple, uint32_t functional_fid,
		   uint32_t *key_count, uint32_t **key_map);

#ifdef __cplusplus
}
#endif

#endif /* TARANTOOL_BOX_FUNCTIONAL_KEY_H_INCLUDED */
