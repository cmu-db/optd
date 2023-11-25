create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int);
create table t3(t3v2 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0), (1), (2);
insert into t3 values (0), (1), (2);

select * from t2, t1, t3 where t1v1 = t2v1 and t1v2 = t3v2;
