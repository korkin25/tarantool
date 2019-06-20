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

#include "lua/decimal.h"
#include "lib/core/decimal.h"
#include "lua/utils.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define LDECIMAL_OP2(name, opname)\
static int \
ldecimal_##name(struct lua_State *L) {\
	if (lua_gettop(L) < 2)\
		return luaL_error(L, "Usage: decimal."#name"(decimal, decimal)");\
	decimal_t *lhs = lua_todecimal(L, 1);\
	decimal_t *rhs = lua_todecimal(L, 2);\
	decimal_t *res = lua_pushdecimal(L);\
	if (decimal_##opname(res, lhs, rhs) == NULL) {\
		lua_pop(L, 1);\
		luaL_error(L, "Operation failed.");\
	}\
	return 1;\
}
#define LDECIMAL_UNOP(name, opname)\
static int \
ldecimal_##name(struct lua_State *L) {\
	if (lua_gettop(L) < 1)\
		return luaL_error(L, "Usage: decimal."#name"(decimal)");\
	decimal_t *lhs = lua_todecimal(L, 1);\
	decimal_t *res = lua_pushdecimal(L);\
	if (decimal_##opname(res, lhs) == NULL) {\
		lua_pop(L, 1);\
		luaL_error(L, "Operation failed");\
	}\
	return 1;\
}

#define LDECIMAL_CMPOP(name, cmp)\
static int \
ldecimal_##name(struct lua_State *L) {\
	if (lua_gettop(L) < 2)\
		return luaL_error(L, "Usage: decimal.__"#name"(decimal, decimal)");\
	decimal_t *lhs = lua_todecimal(L, 1);\
	decimal_t *rhs = lua_todecimal(L, 2);\
	lua_pushboolean(L, decimal_compare(lhs, rhs) cmp 0);\
	return 1;\
}

uint32_t CTID_DECIMAL;

static decimal_t *
lua_pushdecimal(struct lua_State *L)
{
	decimal_t *res = luaL_pushcdata(L, CTID_DECIMAL);
	return res;
}

static decimal_t *
lua_checkdecimal(struct lua_State *L, int index)
{
	uint32_t ctypeid;
	decimal_t *res = luaL_checkcdata(L, index, &ctypeid);
	if (ctypeid != CTID_DECIMAL)
		luaL_error(L, "Expected decimal as %d argument", index);
	return res;
}

static decimal_t *
lua_todecimal(struct lua_State *L, int index)
{
	/*
	 * Convert the index, if it is given relative to the top.
	 * Othervise it will point to a wrong position after
	 * pushdecimal().
	 */
	if (index < 0)
		index = lua_gettop(L) + index + 1;
	decimal_t *res = lua_pushdecimal(L);
	switch(lua_type(L, index))
	{
	case LUA_TNUMBER:
	{
		double n = lua_tonumber(L, index);
		if (decimal_from_double(res, n) == NULL)
			goto err;
		break;
	}
	case LUA_TSTRING:
	{
		const char *str = lua_tostring(L, index);
		if (decimal_from_string(res, str) == NULL)
			goto err;
		break;
	}
	case LUA_TCDATA:
	{
		uint32_t ctypeid;
		void *cdata = luaL_checkcdata(L, index, &ctypeid);
		int64_t ival;
		uint64_t uval;
		double d;
		if (ctypeid == CTID_DECIMAL) {
			/*
			 * We already have a decimal at the
			 * desired position.
			 */
			lua_pop(L, 1);
			return (decimal_t *) cdata;
		}
		switch (ctypeid)
		{
		case CTID_CCHAR:
		case CTID_INT8:
			ival = *(int8_t *) cdata;
			if (decimal_from_int64(res, ival) == NULL)
				goto err;
			break;
		case CTID_INT16:
			ival = *(int16_t *) cdata;
			if (decimal_from_int64(res, ival) == NULL)
				goto err;
			break;
		case CTID_INT32:
			ival = *(int32_t *) cdata;
			if (decimal_from_int64(res, ival) == NULL)
				goto err;
			break;
		case CTID_INT64:
			ival = *(int64_t *) cdata;
			if (decimal_from_int64(res, ival) == NULL)
				goto err;
			break;
		case CTID_UINT8:
			uval = *(uint8_t *) cdata;
			if (decimal_from_uint64(res, uval) == NULL)
				goto err;
			break;
		case CTID_UINT16:
			uval = *(uint16_t *) cdata;
			if (decimal_from_uint64(res, uval) == NULL)
				goto err;
			break;
		case CTID_UINT32:
			uval = *(uint32_t *) cdata;
			if (decimal_from_uint64(res, uval) == NULL)
				goto err;
			break;
		case CTID_UINT64:
			uval = *(uint64_t *) cdata;
			if (decimal_from_uint64(res, uval) == NULL)
				goto err;
			break;
		case CTID_FLOAT:
			d = *(float *) cdata;
			if (decimal_from_double(res, d) == NULL)
				goto err;
			break;
		case CTID_DOUBLE:
			d = *(double *) cdata;
			if (decimal_from_double(res, d) == NULL)
				goto err;
			break;
		default:
			lua_pop(L, 1);
			luaL_error(L, "expected decimal, number or string as %d argument", index);
		}
		break;
	}
	default:
		lua_pop(L, 1);
		luaL_error(L, "expected decimal, number or string as %d argument", index);
	}
	lua_replace(L, index);
	return res;
err:	/* pop the decimal we prepared on top of the stack. */
	lua_pop(L, 1);
	luaL_error(L, "Incorrect value to convert to decimal as %d argument", index);
	/* luaL_error never returns, this is to silence compiler warning. */
	return NULL;
}

LDECIMAL_OP2(add, add);
LDECIMAL_OP2(sub, sub);
LDECIMAL_OP2(mul, mul);
LDECIMAL_OP2(div, div);
LDECIMAL_OP2(pow, pow);

LDECIMAL_UNOP(log10, log10);
LDECIMAL_UNOP(ln, ln);
LDECIMAL_UNOP(exp, exp);
LDECIMAL_UNOP(sqrt, sqrt);
LDECIMAL_UNOP(minus, minus);
LDECIMAL_UNOP(abs, abs);

LDECIMAL_CMPOP(eq, ==);
LDECIMAL_CMPOP(lt, <);
LDECIMAL_CMPOP(le, <=);

static int
ldecimal_tonumber(struct lua_State *L)
{
	if (lua_gettop(L) < 1)
		luaL_error(L, "Usage: decimal.tonumber(value)");
	lua_todecimal(L, 1);
	decimal_t *lhs = lua_checkdecimal(L, 1);
	decimal_t *res = lua_pushdecimal(L);
	*res = *lhs;
	return 1;
}

static int
ldecimal_round(struct lua_State *L)
{
	if (lua_gettop(L) < 2)
		return luaL_error(L, "Usage: decimal.round(decimal, scale)");
	decimal_t *lhs = lua_checkdecimal(L, 1);
	int n = lua_tointeger(L, 2);
	decimal_t *res = lua_pushdecimal(L);
	*res = *lhs;
	decimal_round(res, n);
	return 1;
}

static int
ldecimal_tostring(struct lua_State *L)
{
	if (lua_gettop(L) < 1)
		return luaL_error(L, "Usage: decimal.tostring(decimal)");
	decimal_t *lhs = lua_checkdecimal(L, 1);
	lua_pushstring(L, decimal_to_string(lhs));
	return 1;
}

static const luaL_Reg ldecimal_mt[] = {
	{"log10", ldecimal_log10},
	{"ln", ldecimal_ln},
	{"exp", ldecimal_exp},
	{"sqrt", ldecimal_sqrt},
	{"round", ldecimal_round},
	{"minus", ldecimal_minus},
	{"abs", ldecimal_abs},
	{"tostring", ldecimal_tostring},
	{"__unm", ldecimal_minus},
	{"__add", ldecimal_add},
	{"__sub", ldecimal_sub},
	{"__mul", ldecimal_mul},
	{"__div", ldecimal_div},
	{"__pow", ldecimal_pow},
	{"__eq", ldecimal_eq},
	{"__lt", ldecimal_lt},
	{"__le", ldecimal_le},
	{"__tostring", ldecimal_tostring},
	{NULL, NULL}
};

static const luaL_Reg ldecimal_lib[] = {
	{"eq", ldecimal_eq},
	{"lt", ldecimal_lt},
	{"le", ldecimal_le},
	{"add", ldecimal_add},
	{"sub", ldecimal_sub},
	{"mul", ldecimal_mul},
	{"div", ldecimal_div},
	{"log10", ldecimal_log10},
	{"ln", ldecimal_ln},
	{"pow", ldecimal_pow},
	{"exp", ldecimal_exp},
	{"sqrt", ldecimal_sqrt},
	{"round", ldecimal_round},
	{"minus", ldecimal_minus},
	{"abs", ldecimal_abs},
	{"tostring", ldecimal_tostring},
	{"tonumber", ldecimal_tonumber},
	{NULL, NULL}
};

void
tarantool_lua_decimal_init(struct lua_State *L)
{
	int rc = luaL_cdef(L, "typedef struct {"
				       "int32_t digits;"
				       "int32_t exponent;"
				       "uint8_t bits;"
				       "uint16_t lsu[13];"
			      "} decimal_t;");
	assert(rc == 0);
	(void)rc;
	luaL_register_module(L, "decimal", ldecimal_lib);
	lua_pop(L, 1);
	/*
	 * luaL_metatype is similar to luaL_ctypeid +
	 * luaL_register_type.
	 * The metatable is set automatically to every
	 * cdata of the new ctypeid ever created via ffi.
	 */
	CTID_DECIMAL = luaL_metatype(L, "decimal_t", ldecimal_mt);
	assert(CTID_DECIMAL != 0);
}
