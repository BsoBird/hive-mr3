SET hive.vectorized.execution.enabled=false;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
-- set hive.optimize.sort.dynamic.partition.threshold=1;

create table over1k_n3(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k_n3;

create table over1k_part(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint);

create table over1k_part_limit like over1k_part;

create table over1k_part_buck(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) into 4 buckets;

create table over1k_part_buck_sort(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) 
       sorted by (f) into 4 buckets;

-- map-only jobs converted to map-reduce job by hive.optimize.sort.dynamic.partition optimization
explain insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
explain insert overwrite table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27 limit 10;
explain insert overwrite table over1k_part_buck partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
explain insert overwrite table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;

insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
insert overwrite table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27 limit 10;
insert overwrite table over1k_part_buck partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
insert overwrite table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;




-- map-reduce jobs modified by hive.optimize.sort.dynamic.partition optimization
explain insert into table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
explain insert into table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27 limit 10;
explain insert into table over1k_part_buck partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
explain insert into table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;

insert into table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
insert into table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27 limit 10;
insert into table over1k_part_buck partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
insert into table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;

desc formatted over1k_part partition(ds="foo",t=27);
desc formatted over1k_part partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_limit partition(ds="foo",t=27);
desc formatted over1k_part_limit partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck partition(t=27);
desc formatted over1k_part_buck partition(t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck_sort partition(t=27);
desc formatted over1k_part_buck_sort partition(t="__HIVE_DEFAULT_PARTITION__");

select count(*) from over1k_part;
select count(*) from over1k_part_limit;
select count(*) from over1k_part_buck;
select count(*) from over1k_part_buck_sort;

-- tests for HIVE-6883
create table over1k_part2(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint);

-- set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from over1k_n3 where t is null or t=27 order by i;
-- set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from over1k_n3 where t is null or t=27 order by i;
explain insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from (select * from over1k_n3 order by i limit 10) tmp where t is null or t=27;

-- set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from over1k_n3 where t is null or t=27 group by si,i,b,f,t;
-- set hive.optimize.sort.dynamic.partition.threshold=1;
-- tests for HIVE-8162, only partition column 't' should be in last RS operator
explain insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from over1k_n3 where t is null or t=27 group by si,i,b,f,t;

-- set hive.optimize.sort.dynamic.partition.threshold=-1;
insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from over1k_n3 where t is null or t=27 order by i;

desc formatted over1k_part2 partition(ds="foo",t=27);
desc formatted over1k_part2 partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");

-- SORT_BEFORE_DIFF
select * from over1k_part2;
select count(*) from over1k_part2;

-- set hive.optimize.sort.dynamic.partition.threshold=1;
insert overwrite table over1k_part2 partition(ds="foo",t) select si,i,b,f,t from over1k_n3 where t is null or t=27 order by i;

desc formatted over1k_part2 partition(ds="foo",t=27);
desc formatted over1k_part2 partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");

-- SORT_BEFORE_DIFF
select * from over1k_part2;
select count(*) from over1k_part2;

-- hadoop-1 does not honor number of reducers in local mode. There is always only 1 reducer irrespective of the number of buckets.
-- Hence all records go to one bucket and all other buckets will be empty. Similar to HIVE-6867. However, hadoop-2 honors number
-- of reducers and records are spread across all reducers. To avoid this inconsistency we will make number of buckets to 1 for this test.
create table over1k_part_buck_sort2(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si)
       sorted by (f) into 1 buckets;

-- set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part_buck_sort2 partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;
-- set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert overwrite table over1k_part_buck_sort2 partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;

-- set hive.optimize.sort.dynamic.partition.threshold=-1;
insert overwrite table over1k_part_buck_sort2 partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;

desc formatted over1k_part_buck_sort2 partition(t=27);
desc formatted over1k_part_buck_sort2 partition(t="__HIVE_DEFAULT_PARTITION__");

select * from over1k_part_buck_sort2;
select count(*) from over1k_part_buck_sort2;

-- set hive.optimize.sort.dynamic.partition.threshold=1;
insert overwrite table over1k_part_buck_sort2 partition(t) select si,i,b,f,t from over1k_n3 where t is null or t=27;

desc formatted over1k_part_buck_sort2 partition(t=27);
desc formatted over1k_part_buck_sort2 partition(t="__HIVE_DEFAULT_PARTITION__");

select * from over1k_part_buck_sort2;
select count(*) from over1k_part_buck_sort2;

create table over1k_part3(
           si smallint,
           b bigint,
           f float)
       partitioned by (s string, t tinyint, i int);

-- set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where s="foo";
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where t=27;
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100;
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and t=27;
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and s="foo";
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where t=27 and s="foo";
explain insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and t=27 and s="foo";

insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where s="foo";
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where t=27;
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100;
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and t=27;
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and s="foo";
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where t=27 and s="foo";
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and t=27 and s="foo";

select sum(hash(*)) from over1k_part3;

-- cross verify results with SDPO disabled
drop table over1k_part3;
create table over1k_part3(
           si smallint,
           b bigint,
           f float)
       partitioned by (s string, t tinyint, i int);
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where s="foo";
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where t=27;
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100;
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and t=27;
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and s="foo";
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where t=27 and s="foo";
insert overwrite table over1k_part3 partition(s,t,i) select si,b,f,s,t,i from over1k_n3 where i=100 and t=27 and s="foo";

select sum(hash(*)) from over1k_part3;

drop table over1k_n3;
create table over1k_n3(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k_n3;

analyze table over1k_n3 compute statistics for columns;
set hive.stats.fetch.column.stats=true;

-- default hive should do cost based and add extra RS
-- set hive.optimize.sort.dynamic.partition.threshold=0;
explain insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t>27;

-- default but shouldn't add extra RS
explain insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27 limit 10;

-- disable
-- set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t>27;

-- enable, will add extra RS
-- set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k_n3 where t is null or t=27 limit 10;


create table over1k_part4_0(i int) partitioned by (s string);
create table over1k_part4_1(i int) partitioned by (s string);

EXPLAIN
WITH CTE AS (
select i, s from over1k_n3 where s like 'bob%'
)
FROM (
select * from CTE where i > 1 ORDER BY s
) src1k
insert overwrite table over1k_part4_0 partition(s)
select i+1, s
insert overwrite table over1k_part4_1 partition(s)
select i+0, s
;

WITH CTE AS (
select i, s from over1k_n3 where s like 'bob%'
)
FROM (
select * from CTE where i > 1 ORDER BY s
) src1k
insert overwrite table over1k_part4_0 partition(s)
select i+1, s
insert overwrite table over1k_part4_1 partition(s)
select i+0, s
;

select count(1) from over1k_part4_0;
select count(1) from over1k_part4_1;

drop table over1k_n3;
