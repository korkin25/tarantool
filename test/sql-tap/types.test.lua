#!/usr/bin/env tarantool
test = require("sqltester")
NULL = require('msgpack').NULL
test:plan(55)

--!./tcltestrunner.lua
-- 2001 September 15
--
-- The author disclaims copyright to this source code.  In place of
-- a legal notice, here is a blessing:
--
--    May you do good and not evil.
--    May you find forgiveness for yourself and forgive others.
--    May you share freely, never taking more than you give.
--
-------------------------------------------------------------------------
-- This file implements regression tests for sql library. Specfically
-- it tests that the different storage classes (integer, real, text etc.)
-- all work correctly.
--
-- $Id: types.test,v 1.20 2009/06/29 06:00:37 danielk1977 Exp $
-- ["set","testdir",[["file","dirname",["argv0"]]]]
-- ["source",[["testdir"],"\/tester.tcl"]]
-- Tests in this file are organized roughly as follows:
--
-- types-1.*.*: Test that values are stored using the expected storage
--              classes when various forms of literals are inserted into
--              columns with different affinities.
-- types-1.1.*: INSERT INTO <table> VALUES(...)
-- types-1.2.*: INSERT INTO <table> SELECT...
-- types-1.3.*: UPDATE <table> SET...
--
-- types-2.*.*: Check that values can be stored and retrieving using the
--              various storage classes.
-- types-2.1.*: INTEGER
-- types-2.2.*: REAL
-- types-2.3.*: NULL
-- types-2.4.*: TEXT
-- types-2.5.*: Records with a few different storage classes.
--
-- types-3.*: Test that the '=' operator respects manifest types.
---- Open the table with root-page $rootpage at the btree
---- level. Return a list that is the length of each record
---- in the table, in the tables default scanning order.
--local function record_sizes(rootpage)
--    bt = X(147, "X!cmd", [=[["btree_open","test.db","10"]]=])
--    X(147, "X!cmd", [=[["btree_begin_transaction",["bt"]]]=])
--    c = X(149, "X!cmd", [=[["btree_cursor",["bt"],["rootpage"],"0"]]=])
--    X(149, "X!cmd", [=[["btree_first",["c"]]]=])
--    while 1
-- do
--        table.insert(res,X(153, "X!cmd", [=[["btree_payload_size",["c"]]]=]))
--        if X(154, "X!cmd", [=[["btree_next",["c"]]]=])
-- then
--            break
--        end
--    end
--    X(154, "X!cmd", [=[["btree_close_cursor",["c"]]]=])
--    X(155, "X!cmd", [=[["btree_close",["bt"]]]=])
--    return res
--end

-- Create a table and insert some 1-byte integers. Make sure they
-- can be read back OK. These should be 3 byte records.
test:do_execsql_test(
    "types-2.1.1",
    [[
        CREATE TABLE t1(id  INT primary key, a integer);
        INSERT INTO t1 VALUES(1, 0);
        INSERT INTO t1 VALUES(2, 120);
        INSERT INTO t1 VALUES(3, -120);
    ]], {
        -- <types-2.1.1>
        
        -- </types-2.1.1>
    })

test:do_execsql_test(
    "types-2.1.2",
    [[
        SELECT a FROM t1;
    ]], {
        -- <types-2.1.2>
        0, 120, -120
        -- </types-2.1.2>
    })

-- Try some 2-byte integers (4 byte records)
test:do_execsql_test(
    "types-2.1.3",
    [[
        INSERT INTO t1 VALUES(4, 30000);
        INSERT INTO t1 VALUES(5, -30000);
    ]], {
        -- <types-2.1.3>
        
        -- </types-2.1.3>
    })

test:do_execsql_test(
    "types-2.1.4",
    [[
        SELECT a FROM t1;
    ]], {
        -- <types-2.1.4>
        0, 120, -120, 30000, -30000
        -- </types-2.1.4>
    })

-- 4-byte integers (6 byte records)
test:do_execsql_test(
    "types-2.1.5",
    [[
        INSERT INTO t1 VALUES(6, 2100000000);
        INSERT INTO t1 VALUES(7, -2100000000);
    ]], {
        -- <types-2.1.5>
        
        -- </types-2.1.5>
    })

test:do_execsql_test(
    "types-2.1.6",
    [[
        SELECT a FROM t1;
    ]], {
        -- <types-2.1.6>
        0, 120, -120, 30000, -30000, 2100000000, -2100000000
        -- </types-2.1.6>
    })

-- 8-byte integers (10 byte records)
test:do_execsql_test(
    "types-2.1.7",
    [[
        INSERT INTO t1 VALUES(8, 9000000*1000000*1000000);
        INSERT INTO t1 VALUES(9, -9000000*1000000*1000000);
    ]], {
        -- <types-2.1.7>
        
        -- </types-2.1.7>
    })

test:do_execsql_test(
    "types-2.1.8",
    [[
        SELECT a FROM t1;
    ]], {
        -- <types-2.1.8>
        0, 120, -120, 30000, -30000, 2100000000, -2100000000, 9000000000000000000LL, -9000000000000000000LL
        -- </types-2.1.8>
    })
-- # Check that all the record sizes are as we expected.
-- ifcapable legacyformat {
--   do_test types-2.1.9 {
--     set root [db eval {select rootpage from sql_master where name = 't1'}]
--     record_sizes $root
--   } {3 3 3 4 4 6 6 10 10}
-- } else {
--   do_test types-2.1.9 {
--     set root [db eval {select rootpage from sql_master where name = 't1'}]
--     record_sizes $root
--   } {2 3 3 4 4 6 6 10 10}
-- }
-- Insert some reals. These should be 10 byte records.
test:do_execsql_test(
    "types-2.2.1",
    [[
        CREATE TABLE t2(id  INT primary key, a float);
        INSERT INTO t2 VALUES(1, 0.0);
        INSERT INTO t2 VALUES(2, 12345.678);
        INSERT INTO t2 VALUES(3, -12345.678);
    ]], {
        -- <types-2.2.1>
        
        -- </types-2.2.1>
    })

test:do_execsql_test(
    "types-2.2.2",
    [[
        SELECT a FROM t2;
    ]], {
        -- <types-2.2.2>
        0.0, 12345.678, -12345.678
        -- </types-2.2.2>
    })

-- # Check that all the record sizes are as we expected.
-- ifcapable legacyformat {
--   do_test types-2.2.3 {
--     set root [db eval {select rootpage from sql_master where name = 't2'}]
--     record_sizes $root
--   } {3 10 10}
-- } else {
--   do_test types-2.2.3 {
--     set root [db eval {select rootpage from sql_master where name = 't2'}]
--     record_sizes $root
--   } {2 10 10}
-- }
-- Insert a NULL. This should be a two byte record.
test:do_execsql_test(
    "types-2.3.1",
    [[
        CREATE TABLE t3(id  INT primary key, a INT null);
        INSERT INTO t3 VALUES(1, NULL);
    ]], {
        -- <types-2.3.1>
        
        -- </types-2.3.1>
    })

test:do_execsql_test(
    "types-2.3.2",
    [[
        SELECT a IS NULL FROM t3;
    ]], {
        -- <types-2.3.2>
        1
        -- </types-2.3.2>
    })

-- # Check that all the record sizes are as we expected.
-- do_test types-2.3.3 {
--   set root [db eval {select rootpage from sql_master where name = 't3'}]
--   record_sizes $root
-- } {2}
-- Insert a couple of strings.
local string10 = "abcdefghij"
local string500 = string.rep(string10, 50)
-- MUST_WORK_TEST disabled because in is somewhy very slow
local string500000
if 0>0 then
    string500000 = string.rep(string10, 50000)
else
    string500000 = string.rep(string10, 5000)
end

test:do_test(
    "types-2.4.1",
    function()
        return test:execsql(string.format([[
            CREATE TABLE t4(id  INT primary key, a TEXT);
            INSERT INTO t4 VALUES(1, '%s');
            INSERT INTO t4 VALUES(2, '%s');
            INSERT INTO t4 VALUES(3, '%s');
        ]], string10, string500, string500000))
    end, {
        -- <types-2.4.1>

        -- </types-2.4.1>
    })

test:do_execsql_test(
    "types-2.4.2",
    [[
        SELECT a FROM t4;
    ]], {
        -- <types-2.4.2>
        string10, string500, string500000
        -- </types-2.4.2>
    })

-- Enumeration
CREATE_TABLE = 1
INSERT_VALUES = 2
LIMIT_ARG = 3
OFFSET_ARG = 4
WHERE_ARG = 5
CREATE_INDEX = 6
USEF_FUNC = 7
CAST_TO_INT = 8
CAST_TO_REAL = 9
CAST_TO_TEXT = 10
CAST_TO_BOOL = 11
EQ_TO_INT = 12
EQ_TO_REAL = 13
EQ_TO_TEXT = 14
EQ_TO_BOOL = 15
IN_INT = 16
IN_REAL = 17
IN_TEXT = 18
IN_BOOL = 19
BETWEEN_INT = 20
BETWEEN_REAL = 21
BETWEEN_TEXT = 22
BETWEEN_BOOL = 23
PLUS_INT = 24
PLUS_REAL = 25
PLUS_TEXT = 26
PLUS_BOOL = 27
AND_INT = 28
AND_REAL = 29
AND_TEXT = 30
AND_BOOL = 31
EQP = 32
AGG_FUNC = 33
BUILT_IN_FUNC = 34
CHECK_CONSTRAINT = 35
UNIQUE_CONSTRAINT = 36
FK_CONSTRAINT = 37
AUTOINCREMENT_FIELD = 38

DROP_TABLE = 39

-- Tests
tests = {}
tests[CREATE_TABLE] = "CREATE TABLE t (i %s PRIMARY KEY);"
tests[INSERT_VALUES] = "INSERT INTO t VALUES %s;"
tests[LIMIT_ARG] = "SELECT * FROM t LIMIT %s;"
tests[OFFSET_ARG] = "SELECT * FROM t LIMIT 2 OFFSET %s;"
tests[WHERE_ARG] = "SELECT * FROM t WHERE %s;"
tests[CREATE_INDEX] = "CREATE INDEX i1 ON t(i);"
tests[USEF_FUNC] = "SELECT return_type(i) FROM t LIMIT 1;"
tests[CAST_TO_INT] = "SELECT CAST(i AS INTEGER) FROM t;"
tests[CAST_TO_REAL] = "SELECT CAST(i AS REAL) FROM t;"
tests[CAST_TO_TEXT] = "SELECT CAST(i AS TEXT) FROM t;"
tests[CAST_TO_BOOL] = "SELECT CAST(i AS BOOLEAN) FROM t;"
tests[EQ_TO_INT] = "SELECT i FROM t WHERE i == 1;"
tests[EQ_TO_REAL] = "SELECT i FROM t WHERE i == 1.5;"
tests[EQ_TO_TEXT] = "SELECT i FROM t WHERE i == 'true';"
tests[EQ_TO_BOOL] = "SELECT i FROM t WHERE i == true;"
tests[IN_INT] = "SELECT i FROM t WHERE i IN (1,2,3);"
tests[IN_REAL] = "SELECT i FROM t WHERE i IN (1.1, 2.2, 3.3);"
tests[IN_TEXT] = "SELECT i FROM t WHERE i IN ('a', 'bb', 'ccc');"
tests[IN_BOOL] = "SELECT i FROM t WHERE i IN (true, false);"
tests[BETWEEN_INT] = "SELECT i FROM t WHERE i BETWEEN 1 AND 3;"
tests[BETWEEN_REAL] = "SELECT i FROM t WHERE i BETWEEN 1.5 AND 3.5;"
tests[BETWEEN_TEXT] = "SELECT i FROM t WHERE i BETWEEN 'a' AND 'g';"
tests[BETWEEN_BOOL] = "SELECT i FROM t WHERE i BETWEEN true AND false;"
tests[PLUS_INT] = "SELECT i + 1 FROM t LIMIT 2;"
tests[PLUS_REAL] = "SELECT i + 1.5 FROM t LIMIT 2;"
tests[PLUS_TEXT] = "SELECT i + 'a' FROM t LIMIT 2;"
tests[PLUS_BOOL] = "SELECT i + true FROM t LIMIT 2;"
tests[AND_INT] = "SELECT i AND 1 FROM t LIMIT 2;"
tests[AND_REAL] = "SELECT i AND 1.5 FROM t LIMIT 2;"
tests[AND_TEXT] = "SELECT i AND 'a' FROM t LIMIT 2;"
tests[AND_BOOL] = "SELECT i AND true FROM t LIMIT 2;"
tests[EQP] = "EXPLAIN QUERY PLAN SELECT i FROM t WHERE i = %s;"
tests[AGG_FUNC] = "SELECT count(i) FROM t;"
tests[BUILT_IN_FUNC] = "SELECT typeof(i) FROM t LIMIT 1;"
tests[CHECK_CONSTRAINT] = [[
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (i INT PRIMARY KEY, a %s, CONSTRAINT ck CHECK(a %s));
    INSERT INTO t VALUES (1, %s);
]]
tests[UNIQUE_CONSTRAINT] = [[
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (i INT PRIMARY KEY, a %s, CONSTRAINT uq UNIQUE(a));
    INSERT INTO t VALUES (1, %s), (2, %s);
]]
tests[FK_CONSTRAINT] = [[
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (i INT PRIMARY KEY, a %s UNIQUE, b %s, CONSTRAINT fk FOREIGN KEY(b) REFERENCES t(a));
    INSERT INTO t VALUES (1, %s, %s);
]]
tests[AUTOINCREMENT_FIELD] = [[
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (i %s PRIMARY KEY AUTOINCREMENT);
    INSERT INTO t VALUES (%s);
    INSERT INTO t VALUES (NULL);
    SELECT * FROM t;
]]
tests[DROP_TABLE] = "DROP TABLE IF EXISTS t;"

suits = {}
-- Type: BOOLEAN
suits.bool = {}
suits.bool[CREATE_TABLE] = {args = {"BOOLEAN"}, result = {0}}
suits.bool[INSERT_VALUES] = {args = {"(true), (false)"}, result = {0}}
suits.bool[LIMIT_ARG] = {args = {"true"}, result = {1, 'Failed to execute SQL statement: Only positive integers are allowed in the LIMIT clause'}}
suits.bool[OFFSET_ARG] = {args = {"false"}, result = {1, 'Failed to execute SQL statement: Only positive integers are allowed in the OFFSET clause'}}
suits.bool[WHERE_ARG] = {args = {"i"}, result = {0, {true}}}
suits.bool[CREATE_INDEX] = {args = {}, result = {0}}
suits.bool[USEF_FUNC] = {args = {}, result = {0, {"boolean"}}}
suits.bool[CAST_TO_INT] = {args = {}, result = {0, {0, 1}}}
suits.bool[CAST_TO_REAL] = {args = {}, result = {1, "Type mismatch: can not convert false to number"}}
suits.bool[CAST_TO_TEXT] = {args = {}, result = {0, {"FALSE", "TRUE"}}}
suits.bool[CAST_TO_BOOL] = {args = {}, result = {0, {false,true}}}
suits.bool[EQ_TO_INT] = {args = {}, result = {1, "Type mismatch: can not convert INTEGER to boolean"}}
suits.bool[EQ_TO_REAL] = {args = {}, result = {1, "Type mismatch: can not convert REAL to boolean"}}
suits.bool[EQ_TO_TEXT] = {args = {}, result = {1, "Supplied key type of part 0 does not match index part type: expected boolean"}}
suits.bool[EQ_TO_BOOL] = {args = {}, result = {0, {true}}}
suits.bool[IN_INT] = {args = {}, result = {1, "Type mismatch: can not convert 1 to boolean"}}
suits.bool[IN_REAL] = {args = {}, result = {1, "Type mismatch: can not convert 1.1 to boolean"}}
suits.bool[IN_TEXT] = {args = {}, result = {1, "Type mismatch: can not convert a to boolean"}}
suits.bool[IN_BOOL] = {args = {}, result = {0, {false, true}}}
suits.bool[BETWEEN_INT] = {args = {}, result = {1, "Type mismatch: can not convert INTEGER to boolean"}}
suits.bool[BETWEEN_REAL] = {args = {}, result = {1, "Type mismatch: can not convert REAL to boolean"}}
suits.bool[BETWEEN_TEXT] = {args = {}, result = {1, "Supplied key type of part 0 does not match index part type: expected boolean"}}
suits.bool[BETWEEN_BOOL] = {args = {}, result = {0, {}}}
suits.bool[PLUS_INT] = {args = {}, result = {1, "Type mismatch: can not convert false to numeric"}}
suits.bool[PLUS_REAL] = {args = {}, result = {1, "Type mismatch: can not convert false to numeric"}}
suits.bool[PLUS_TEXT] = {args = {}, result = {1, "Type mismatch: can not convert a to numeric"}}
suits.bool[PLUS_BOOL] = {args = {}, result = {1, "Type mismatch: can not convert true to numeric"}}
suits.bool[AND_INT] = {args = {}, result = {1, "Type mismatch: can not convert 1 to boolean"}}
suits.bool[AND_REAL] = {args = {}, result = {1, "Type mismatch: can not convert 1.5 to boolean"}}
suits.bool[AND_TEXT] = {args = {}, result = {1, "Type mismatch: can not convert a to boolean"}}
suits.bool[AND_BOOL] = {args = {}, result = {0, {false, true}}}
suits.bool[EQP] = {args = {'true'}, result = {0, {0, 0, 0, "SEARCH TABLE T USING PRIMARY KEY (I=?) (~1 row)"}}}
suits.bool[AGG_FUNC] = {args = {}, result = {0, {2}}}
suits.bool[BUILT_IN_FUNC] = {args = {}, result = {0, {'boolean'}}}
suits.bool[CHECK_CONSTRAINT] = {args = {'BOOLEAN', '!= TRUE', 'true'}, result = {1, "Check constraint failed 'CK': a != TRUE"}}
suits.bool[UNIQUE_CONSTRAINT] = {args = {'BOOLEAN', 'true', 'true'}, result = {1, "Duplicate key exists in unique index 'unique_UQ_2' in space 'T'"}}
suits.bool[FK_CONSTRAINT] = {args = {'BOOLEAN', 'BOOLEAN', 'true', 'false'}, result = {1, "Failed to execute SQL statement: FOREIGN KEY constraint failed"}}
suits.bool[AUTOINCREMENT_FIELD] = {args = {'BOOLEAN', 'false'}, result = {1, "Failed to create space 'T': AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY or INT PRIMARY KEY"}}
suits.bool[DROP_TABLE] = {args = {}, result = {0}}

local function return_type(arg)
    return type(arg)
end

box.internal.sql_create_function("return_type", "TEXT", return_type)

i = 0
for k,v in pairs(suits) do
    i = i + 1
    for j = 1,#tests do
        test:do_catchsql_test(
            "types-3."..i.."."..j,
            string.format(tests[j], unpack(v[j].args)),
            v[j].result)
    end
end

i = 0
suits = {}
suits.bool = {'BOOLEAN', '(true), (false)', 'true'}

for _,v in pairs(suits) do
    i = i + 1
    test:do_test(
        "types-4.1."..i,
        function()
            result = test:execsql(string.format([[
                CREATE TABLE t(id %s PRIMARY KEY);
                INSERT INTO t VALUES %s;
                EXPLAIN SELECT * FROM (VALUES (%s)), t;
            ]], unpack(v)))
            for _,w in pairs(result) do
                if w == "IteratorOpen" then
                    return true
                end
            end
            return false
        end,
        true
    )
end

i = 0
suits = {}
suits.bool = {args = {true}, result = {{'BOOLEAN', 'string'}, {"true", "boolean"}}}

for _,v in pairs(suits) do
    i = i + 1
    test:do_test(
        "types-4.2."..i,
        function()
            result = box.execute('SELECT ?, return_type($1)', v.args)
            types = {result.metadata[1].type, result.metadata[2].type}
            values = {tostring(result.rows[1][1]), tostring(result.rows[1][2])}
            return {types, values}
        end,
        v.result
    )
end

test:finish_test()
