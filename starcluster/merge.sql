/*
    Merge together two disctinct MySQL tables
*/

insert into patent select * from uspto_2.patent;
insert ignore into location select * from uspto_2.location;
insert ignore into subclass select * from uspto_2.subclass;
insert ignore into mainclass select * from uspto_2.mainclass;
insert into application select * from uspto_2.application;
insert into assignee select * from uspto_2.assignee;
insert into citation select * from uspto_2.citation;
insert into inventor select * from uspto_2.inventor;
insert into ipcr select * from uspto_2.ipcr;
insert into lawyer select * from uspto_2.lawyer;
insert into otherreference select * from uspto_2.otherreference;
insert into usreldoc select * from uspto_2.usreldoc;
insert into uspc select * from uspto_2.uspc;
