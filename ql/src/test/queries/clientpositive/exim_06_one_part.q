set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee_n3;

create table exim_employee_n3 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n3 partition (emp_country="in", emp_state="tn");	
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n3 partition (emp_country="in", emp_state="ka");	
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n3 partition (emp_country="us", emp_state="tn");	
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n3 partition (emp_country="us", emp_state="ka");		
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee_n3 partition (emp_country="in",emp_state="ka") to 'ql/test/data/exports/exim_employee';
drop table exim_employee_n3;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_employee';
describe extended exim_employee_n3;
show table extended like exim_employee_n3;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_employee;
select * from exim_employee_n3;
drop table exim_employee_n3;

drop database importer;
