--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=1;
-- set hive.optimize.cte.materialize.full.aggregate.only=false;

-- union test
with q1 as (select * from src where key= '5'),
q2 as (select * from src s2 where key = '4')
select * from q1 union all select * from q2
;

-- insert test
create table s1_n0 like src;
with q1 as ( select key, value from src where key = '5')
from q1
insert overwrite table s1_n0
select *
;
select * from s1_n0;
drop table s1_n0;

-- from style
with q1 as (select * from src where key= '5')
from q1
select *
;

-- ctas
create table s2 as
with q1 as ( select key from src where key = '4')
select * from q1
;

select * from s2;
drop table s2;

-- view test
create view v1_n4 as
with q1 as ( select key from src where key = '5')
select * from q1
;

select * from v1_n4;

drop view v1_n4;


-- view test, name collision
create view v1_n4 as
with q1 as ( select key from src where key = '5')
select * from q1
;

with q1 as ( select key from src where key = '4')
select * from v1_n4
;

drop view v1_n4;
