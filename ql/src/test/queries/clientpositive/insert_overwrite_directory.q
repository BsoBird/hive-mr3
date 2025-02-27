--! qt:dataset:src
insert overwrite directory '../../data/files/src_table_1'
select * from src ;
dfs -cat ../../data/files/src_table_1/000000_0;

insert overwrite directory '../../data/files/src_table_2'
row format delimited 
FIELDS TERMINATED BY ':' 
select * from src ;

dfs -cat ../../data/files/src_table_2/000000_0;

create table array_table_n1 (a array<string>, b array<string>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ',';

load data local inpath "../../data/files/array_table.txt" overwrite into table array_table_n1;

insert overwrite directory '../../data/files/array_table_1'
select * from array_table_n1;
dfs -cat ../../data/files/array_table_1/000000_0;

insert overwrite directory '../../data/files/array_table_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
select * from array_table_n1;

dfs -cat ../../data/files/array_table_2/000000_0;

insert overwrite directory '../../data/files/array_table_2_withfields'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
select b,a from array_table_n1;

dfs -cat ../../data/files/array_table_2_withfields/000000_0;


create table map_table_n2 (foo STRING , bar MAP<STRING, STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

load data local inpath "../../data/files/map_table.txt" overwrite into table map_table_n2;

insert overwrite directory '../../data/files/map_table_1'
select * from map_table_n2;
dfs -cat ../../data/files/map_table_1/000000_0;

insert overwrite directory '../../data/files/map_table_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY '='
select * from map_table_n2;

dfs -cat ../../data/files/map_table_2/000000_0;

insert overwrite directory '../../data/files/map_table_2_withfields'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY '='
select bar,foo from map_table_n2;

dfs -cat ../../data/files/map_table_2_withfields/000000_0;

insert overwrite directory '../../data/files/array_table_3'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.DelimitedJSONSerDe'
STORED AS TEXTFILE
select * from array_table_n1;

dfs -cat ../../data/files/array_table_3/000000_0;


insert overwrite directory '../../data/files/array_table_4'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'serialization.format'= 'org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol',
'quote.delim'= '(\"|\\[|\\])',  'field.delim'=',',
'serialization.null.format'='-NA-', 'collection.delim'='#') STORED AS TEXTFILE
select a, null, b from array_table_n1;

dfs -cat ../../data/files/array_table_4/000000_0;

insert overwrite directory '../../data/files/map_table_3'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.DelimitedJSONSerDe'
STORED AS TEXTFILE
select * from map_table_n2;

dfs -cat ../../data/files/map_table_3/000000_0;

insert overwrite directory '../../data/files/map_table_4'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'serialization.format'= 'org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol',
'quote.delim'= '(\"|\\[|\\])',  'field.delim'=':',
'serialization.null.format'='-NA-', 'collection.delim'='#', 'mapkey.delim'='%') STORED AS TEXTFILE
select foo, null, bar from map_table_n2;

dfs -cat ../../data/files/map_table_4/000000_0;

insert overwrite directory '../../data/files/rctable'
STORED AS RCFILE
select value,key from src;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/rctable/temp;
dfs -rm -r -f ${system:test.tmp.dir}/rctable;
dfs ${system:test.dfs.mkdir}  ${system:test.tmp.dir}/rctable;
dfs -put ../../data/files/rctable/000000_0 ${system:test.tmp.dir}/rctable/000000_0;

create external table rctable(value string, key string)
STORED AS RCFILE
LOCATION '${system:test.tmp.dir}/rctable';

insert overwrite directory '../../data/files/rctable_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
select key,value from rctable;

dfs -cat ../../data/files/rctable_out/000000_0;

-- test iow directory when query result cache is enabled
set hive.query.results.cache.enabled=true;
insert overwrite directory '../../data/files/iowd_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
select key,value from rctable;

dfs -cat ../../data/files/iowd_out/000000_0;

drop table rctable;
drop table array_table_n1;
drop table map_table_n2;
dfs -rm -r -f ${system:test.tmp.dir}/rctable;
dfs -rm -r -f ../../data/files/array_table_1;
dfs -rm -r -f ../../data/files/array_table_2;
dfs -rm -r -f ../../data/files/array_table_3;
dfs -rm -r -f ../../data/files/array_table_4;
dfs -rm -r -f ../../data/files/map_table_1;
dfs -rm -r -f ../../data/files/map_table_2;
dfs -rm -r -f ../../data/files/map_table_3;
dfs -rm -r -f ../../data/files/map_table_4;
dfs -rm -r -f ../../data/files/rctable;
dfs -rm -r -f ../../data/files/rctable_out;
dfs -rm -r -f ../../data/files/src_table_1;
dfs -rm -r -f ../../data/files/src_table_2;
dfs -rm -r -f ../../data/files/iowd_out;
