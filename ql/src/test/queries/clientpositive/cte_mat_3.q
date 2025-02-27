--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=2;
-- set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.explain.user=true;

explain
with q1 as (select * from src where key= '5')
select a.key
from q1 a join q1 b
on a.key=b.key;
