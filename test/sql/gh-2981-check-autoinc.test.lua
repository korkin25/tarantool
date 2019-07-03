test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.execute('pragma sql_default_engine=\''..engine..'\'')

box.cfg{}

box.execute("CREATE TABLE t1 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 <> 19));");
box.execute("CREATE TABLE t2 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 <> 19 AND s1 <> 25));");
box.execute("CREATE TABLE t3 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 < 10));");
box.execute("CREATE TABLE t4 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 < 10));");
box.execute("CREATE TABLE t5 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 <> 19));");

box.execute("insert into t1 values (18, null);")
box.execute("insert into t1(s2) values (null);")

box.execute("insert into t2 values (18, null);")
box.execute("insert into t2(s2) values (null);")
box.execute("insert into t2 values (24, null);")
box.execute("insert into t2(s2) values (null);")

box.execute("insert into t3 values (9, null)")
box.execute("insert into t3(s2) values (null)")

box.execute("insert into t4 values (9, null)")
box.execute("insert into t4 values (null, null)")

box.execute("INSERT INTO t5 VALUES (18, NULL);")
box.execute("INSERT INTO t5 SELECT NULL, NULL FROM t5;")

box.execute("DROP TABLE t1")
box.execute("DROP TABLE t2")
box.execute("DROP TABLE t3")
box.execute("DROP TABLE t4")
box.execute("DROP TABLE t5")
