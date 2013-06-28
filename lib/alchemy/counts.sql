.mode columns
select "patent", count(*) from patent;
select "applicat", count(*) from application;
select "citation", count(*) from citation;
select "inventor", count(*) from inventor;
select "maincls", count(*) from mainclass;
select "subclass", count(*) from subclass;
select "assignee", count(*) from assignee;
select "lawyer", count(*) from lawyer;
select "otherref", count(*) from otherreference;
select "uspc", count(*) from uspc;
select "location", count(*) from location;
select "usreldoc", count(*) from usreldoc;
