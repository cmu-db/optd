-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- Test whether we can transpose filter and projection
SELECT t1.t1v1, t1.t1v2, t2.t2v3
  FROM t1, t2
  WHERE t1.t1v1 = t2.t2v1;

/*
Error
Unknown logical rule: {"project_filter_transpose_rule"}
*/

