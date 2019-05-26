drop schema if exists national_electric;
create database national_electric;
create table national_electric.employee (id int not null primary key auto_increment, first_name varchar(20), last_name varchar(20), department varchar(30)) engine=InnoDB auto_increment=1 default charset=utf8;