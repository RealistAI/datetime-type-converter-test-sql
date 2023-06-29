create table acme.employee(eid int, did int, name varchar(10), jd int);
insert into acme.employee select 42, 1, 'Acme', 20011213;
select eid, did, name, jd from acme.employee e where jd > 20010101;

