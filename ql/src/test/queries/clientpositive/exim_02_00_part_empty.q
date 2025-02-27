set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee_n9;

create table exim_employee_n9 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee_n9 to 'ql/test/data/exports/exim_employee';
drop table exim_employee_n9;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_employee';
describe extended exim_employee_n9;
show table extended like exim_employee_n9;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_employee;
select * from exim_employee_n9;
drop table exim_employee_n9;

drop database importer;
