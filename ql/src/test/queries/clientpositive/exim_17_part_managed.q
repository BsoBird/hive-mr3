set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee_n4;

create table exim_employee_n4 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n4 partition (emp_country="in", emp_state="tn");	
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n4 partition (emp_country="in", emp_state="ka");	
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n4 partition (emp_country="us", emp_state="tn");	
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n4 partition (emp_country="us", emp_state="ka");		
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee_n4 to 'ql/test/data/exports/exim_employee';
drop table exim_employee_n4;

create database importer;
use importer;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/tablestore/exim_employee/temp;
dfs -rm -r -f target/tmp/ql/test/data/tablestore/exim_employee;

create table exim_employee_n4 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
import table exim_employee_n4 partition (emp_country="us", emp_state="tn") 
	from 'ql/test/data/exports/exim_employee'
	location 'ql/test/data/tablestore/exim_employee';
alter table exim_employee_n4 add partition	(emp_country="us", emp_state="ap")
	location 'ql/test/data/tablestore2/exim_employee';
show table extended like exim_employee_n4;
show table extended like exim_employee_n4 partition (emp_country="us", emp_state="tn");
show table extended like exim_employee_n4 partition (emp_country="us", emp_state="ap");	
dfs -rm -r -f target/tmp/ql/test/data/exports/exim_employee;
select * from exim_employee_n4;
dfs -rm -r -f target/tmp/ql/test/data/tablestore/exim_employee;
select * from exim_employee_n4;
drop table exim_employee_n4;

drop database importer;
