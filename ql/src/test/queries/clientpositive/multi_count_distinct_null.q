set hive.mapred.mode=nonstrict;

drop table employee;

create table employee (department_id int, gender varchar(10), education_level int);

explain
select count(distinct 0), count(distinct null) from employee;

select count(distinct 0), count(distinct null) from employee;

insert into employee values (1, 'M', 1),(1, 'M', 1),(2, 'F', 1),(1, 'F', 3),(1, 'M', 2),(4, 'M', 1),(2, 'F', 1),(2, 'F', 3),(3, 'M', 2),(null, 'M', 1),(null, null, 1),(null, null, null);

select count(distinct 0), count(distinct null) from employee;

explain select count(distinct department_id), count(distinct gender), count(distinct education_level) from employee;

select count(distinct gender), count(distinct department_id), count(distinct education_level) from employee;

select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level) from employee;

select count(distinct department_id), count(distinct gender), count(distinct education_level) from employee;

select count(distinct department_id), count(distinct gender), count(distinct education_level), count(distinct education_level) from employee;

select count(distinct department_id), count(distinct gender), count(distinct education_level), 
count(distinct education_level, department_id) from employee;

select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level),
count(distinct education_level, department_id), count(distinct department_id, education_level) from employee;

explain select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level),
count(distinct education_level, department_id), count(distinct department_id, education_level), count(distinct department_id, education_level, gender) from employee;

select count(distinct gender), count(distinct department_id), count(distinct gender), count(distinct education_level),
count(distinct education_level, department_id), count(distinct department_id, education_level), count(distinct department_id, education_level, gender) from employee;

select 
count(case when i=3 and department_id is not null then 1 else null end) as c0, 
count(case when i=5 and gender is not null then 1 else null end) as c1, 
count(case when i=6 and education_level is not null then 1 else null end) as c2 from 
(select grouping__id as i, department_id, gender, 
education_level from employee group by department_id, gender, education_level grouping sets 
(department_id, gender, education_level))subq;

select grouping__id as i, department_id, gender, education_level from employee 
group by department_id, gender, education_level grouping sets 
(department_id, gender, education_level, (education_level, department_id));




