--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=true;

SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.auto.convert.join=false;

DROP TABLE orc_llap_n0;

set hive.auto.convert.join=true;
SET hive.llap.io.enabled=true;

CREATE TABLE orc_llap_n0(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN,
    cdecimal1 decimal(10,2),
    cdecimal2 decimal(38,5))
    STORED AS ORC tblproperties ("orc.compress"="NONE");

insert into table orc_llap_n0
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2,
 cast("3.345" as decimal(10,2)), cast("5.56789" as decimal(38,5)) from alltypesorc;

alter table orc_llap_n0 set tblproperties ("orc.compress"="NONE", 'orc.write.format'='UNSTABLE-PRE-2.0');

insert into table orc_llap_n0
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2,
 cast("3.345" as decimal(10,2)), cast("5.56789" as decimal(38,5)) from alltypesorc;

SET hive.llap.io.enabled=true;

drop table llap_temp_table;
explain
select * from orc_llap_n0 where cint > 10 and cbigint is not null;
create table llap_temp_table as
select * from orc_llap_n0 where cint > 10 and cbigint is not null;
select sum(hash(*)) from llap_temp_table;

explain
select * from orc_llap_n0 where cint > 10 and cint < 5000000;
select * from orc_llap_n0 where cint > 10 and cint < 5000000;

DROP TABLE orc_llap_n0;
drop table llap_temp_table;
