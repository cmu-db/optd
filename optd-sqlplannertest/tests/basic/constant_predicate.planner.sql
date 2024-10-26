-- (no id or description)
create table t1(t1v1 int, t1v2 int);
insert into t1 values (0, 0), (1, 1), (2, 2);

/*
3
*/

-- Test whether the optimizer handles integer equality predicates correctly.
select * from t1 where t1v1 = 0;

/*
0 0
*/

-- Test whether the optimizer handles multiple integer equality predicates correctly.
select * from t1 where t1v1 = 0 and t1v2 = 1;

/*

*/

-- Test whether the optimizer handles multiple integer inequality predicates correctly.
select * from t1 where t1v1 = 0 and t1v2 != 1;

/*
0 0
*/

