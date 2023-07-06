CREATE TABLE EMPLOYEE (
	EID INT,
	boss varchar(100),
	name char(10),
	jd int,
	lastname char(10),
	firstname char(10)
	)
	;

INSERT into employee select 
	42, 
	'Matthias Gilbert', 
	'Acme', 
	80095,
	'Huizinga', 'Gregory'
	;

SELECT eid, 
	boss, 
	name, 
	jd 
	from employee e
	WHERE jd > 80000
