--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=true;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE orc_llap_n2;

CREATE TABLE orc_llap_n2 (
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cint0 INT,
    cbigint0 BIGINT,
    cfloat0 FLOAT,
    cdouble0 DOUBLE,
    cint1 INT,
    cbigint1 BIGINT,
    cfloat1 FLOAT,
    cdouble1 DOUBLE,
    cstring1 string,
    cfloat2 float,
    cdecimal1 decimal(10,3),
    cdecimal2 decimal(38,10)
)  stored as orc TBLPROPERTIES ('transactional'='true','orc.write.format'='UNSTABLE-PRE-2.0');


insert into table orc_llap_n2
select cint, cbigint, cfloat, cdouble,
 cint as c1, cbigint as c2, cfloat as c3, cdouble as c4,
 cint as c8, cbigint as c7, cfloat as c6, cdouble as c5,
 cstring1, cfloat as c9, cast("1.123" as decimal(10,3))as c10,
 cast("1.123456789" as decimal(38,18)) as c11
from alltypesorc
order by cint, cbigint, cfloat, cdouble, cstring1
limit 30;

alter table orc_llap_n2 set TBLPROPERTIES ('transactional'='true','orc.write.format'='0.12');

insert into table orc_llap_n2
select cint, cbigint, cfloat, cdouble,
 cint as c1, cbigint as c2, cfloat as c3, cdouble as c4,
 cint as c8, cbigint as c7, cfloat as c6, cdouble as c5,
 cstring1, cfloat as c9, cast("3.321" as decimal(10,3))as c10,
 cast("9.987654321" as decimal(38,18)) as c11
from alltypesorc
order by cint, cbigint, cfloat, cdouble, cstring1
limit 30;


CREATE TABLE orc_llap2 (
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cint0 INT,
    cbigint0 BIGINT,
    cfloat0 FLOAT,
    cdouble0 DOUBLE,
    cint1 INT,
    cbigint1 BIGINT,
    cfloat1 FLOAT,
    cdouble1 DOUBLE,
    cstring1 string,
    cfloat2 float,
    cdecimal1 decimal(10,3),
    cdecimal2 decimal(38,10)
)  stored as orc TBLPROPERTIES ('transactional'='false', 'orc.write.format'='UNSTABLE-PRE-2.0');

insert into table orc_llap2
select cint, cbigint, cfloat, cdouble,
 cint as c1, cbigint as c2, cfloat as c3, cdouble as c4,
 cint as c8, cbigint as c7, cfloat as c6, cdouble as c5,
 cstring1, cfloat as c9, cast("1.123" as decimal(10,3))as c10,
 cast("1.123456789" as decimal(38,18)) as c11
from alltypesorc
order by cint, cbigint, cfloat, cdouble, cstring1
limit 30;

alter table orc_llap2 set TBLPROPERTIES ('transactional'='true','orc.write.format'='0.12');

update orc_llap2 set cstring1 = 'testvalue', cdecimal1 = cast("3.321" as decimal(10,3)),
cdecimal2 = cast("9.987654321" as decimal(38,18))  where cstring1 = 'N016jPED08o';


SET hive.llap.io.enabled=true;

-- SORT_QUERY_RESULTS

select cstring1 from orc_llap_n2;
select cfloat2, cint from orc_llap_n2;
select * from orc_llap_n2;

select cstring1 from orc_llap2;
select cfloat2, cint from orc_llap2;
select * from orc_llap2;


DROP TABLE orc_llap_n2;
