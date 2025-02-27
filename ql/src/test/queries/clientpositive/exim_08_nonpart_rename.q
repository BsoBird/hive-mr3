set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n9,exim_employee,exim_imported_dept;

create table exim_department_n9 ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" into table exim_department_n9;		
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n9 to 'ql/test/data/exports/exim_department';
drop table exim_department_n9;

create database importer;
use importer;
create table exim_department_n9 ( dep_id int comment "department id") 	
	partitioned by (emp_org string)
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" into table exim_department_n9 partition (emp_org="hr");
import table exim_imported_dept from 'ql/test/data/exports/exim_department';
describe extended exim_imported_dept;
select * from exim_imported_dept;
drop table exim_imported_dept;
drop table exim_department_n9;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_department;

drop database importer;
