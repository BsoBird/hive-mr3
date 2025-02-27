--! qt:dataset:src
-- set hive.stats.filter.range.uniform=false;
set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=30000;
set hive.llap.memory.oversubscription.max.executors.per.query=3;

CREATE TABLE srcbucket_mapjoin_n18(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n11 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n20 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n18 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n18 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n20 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n20 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n20 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n20 partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n11 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n20;

CREATE TABLE tab_n10(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n10 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n18;

analyze table srcbucket_mapjoin_n18 compute statistics for columns;
analyze table srcbucket_mapjoin_part_n20 compute statistics for columns;
analyze table tab_n10 compute statistics for columns;
analyze table tab_part_n11 compute statistics for columns;

set hive.auto.convert.join.noconditionaltask.size=3500;
set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.key, b.key from tab_part_n11 a join tab_part_n11 c on a.key = c.key join tab_part_n11 b on a.value = b.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, b.key from tab_part_n11 a join tab_part_n11 c on a.key = c.key join tab_part_n11 b on a.value = b.value;

CREATE TABLE tab1_n5(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab1_n5
select key,value from srcbucket_mapjoin_n18;
analyze table tab1_n5 compute statistics for columns;

-- A negative test as src is not bucketed.
set hive.auto.convert.join.noconditionaltask.size=12000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, a.value, b.value
from tab1_n5 a join src b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab1_n5 a join src b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=2500;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, b.key from (select key from tab_part_n11 where key > 1) a join (select key from tab_part_n11 where key > 2) b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, b.key from (select key from tab_part_n11 where key > 1) a join (select key from tab_part_n11 where key > 2) b on a.key = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, b.key from (select key from tab_part_n11 where key > 1) a left outer join (select key from tab_part_n11 where key > 2) b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, b.key from (select key from tab_part_n11 where key > 1) a left outer join (select key from tab_part_n11 where key > 2) b on a.key = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, b.key from (select key from tab_part_n11 where key > 1) a right outer join (select key from tab_part_n11 where key > 2) b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, b.key from (select key from tab_part_n11 where key > 1) a right outer join (select key from tab_part_n11 where key > 2) b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=2000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.key, b.key from (select distinct key from tab_n10) a join tab_n10 b on b.key = a.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, b.key from (select distinct key from tab_n10) a join tab_n10 b on b.key = a.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.value, b.value from (select distinct value from tab_n10) a join tab_n10 b on b.key = a.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.value, b.value from (select distinct value from tab_n10) a join tab_n10 b on b.key = a.value;



--multi key
CREATE TABLE tab_part1 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key, value) INTO 4 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_part1 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n20;
analyze table tab_part1 compute statistics for columns;

set hive.auto.convert.join.noconditionaltask.size=12000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select count(*)
from
(select distinct key,value from tab_part_n11) a join tab_n10 b on a.key = b.key and a.value = b.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select count(*)
from
(select distinct key,value from tab_part_n11) a join tab_n10 b on a.key = b.key and a.value = b.value;


--HIVE-17939
create table small (i int) stored as ORC;
create table big (i int) partitioned by (k int) clustered by (i) into 10 buckets stored as ORC;

insert into small values (1),(2),(3),(4),(5),(6);
insert into big partition(k=1) values(1),(3),(5),(7),(9);
insert into big partition(k=2) values(0),(2),(4),(6),(8);
explain select small.i, big.i from small,big where small.i=big.i;
select small.i, big.i from small,big where small.i=big.i order by small.i, big.i;

-- Bucket map join disabled for external tables
-- Create external table equivalent of tab_part_n11
CREATE EXTERNAL TABLE tab_part_ext (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_part_ext partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n20;
analyze table tab_part_ext compute statistics for columns;

set hive.auto.convert.join.noconditionaltask.size=3500;
set hive.convert.join.bucket.mapjoin.tez = true;
set hive.disable.unsafe.external.table.operations=true;
set test.comment=Bucket map join should work here;
set test.comment;
explain select a.key, b.key from tab_part_n11 a join tab_part_n11 c on a.key = c.key join tab_part_n11 b on a.value = b.value;

set test.comment=External tables, bucket map join should be disabled;
set test.comment;
explain select a.key, b.key from tab_part_ext a join tab_part_ext c on a.key = c.key join tab_part_ext b on a.value = b.value;

-- HIVE-20187 : Must not create BMJ
create table my_fact(AMT decimal(20,3),bucket_col string ,join_col string )
PARTITIONED BY (FISCAL_YEAR string ,ACCOUNTING_PERIOD string )
CLUSTERED BY (bucket_col) INTO 10
BUCKETS
stored as ORC
;
create table my_dim(join_col string,filter_col string) stored as orc;

INSERT INTO my_dim VALUES("1", "VAL1"), ("2", "VAL2"), ("3", "VAL3"), ("4", "VAL4");
INSERT OVERWRITE TABLE my_fact PARTITION(FISCAL_YEAR="2015", ACCOUNTING_PERIOD="20") VALUES(1.11, "20", "1"), (1.11, "20", "1"), (1.12, "20", "2"), (1.12, "20", "3"), (1.12, "11", "3"), (1.12, "9", "3");

explain  extended
select bucket_col, my_dim.join_col as account1,my_fact.accounting_period
FROM my_fact JOIN my_dim ON my_fact.join_col = my_dim.join_col
WHERE my_fact.fiscal_year = '2015'
AND my_dim.filter_col IN ( 'VAL1', 'VAL2' )
and my_fact.accounting_period in (10);

reset hive.llap.memory.oversubscription.max.executors.per.query;
