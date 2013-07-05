/*
    Merge together two disctinct MySQL tables
    mysqldump [options] uspto -T /var/lib/mysql/uspto
*/

SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET SESSION tx_isolation='READ-UNCOMMITTED';
SET autocommit=0; 

LOAD DATA LOCAL INFILE '[dir]/patent.txt' INTO TABLE patent FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/location.txt' IGNORE INTO TABLE location FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/subclass.txt' IGNORE INTO TABLE subclass FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/mainclass.txt' IGNORE INTO TABLE mainclass FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/application.txt' INTO TABLE application FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/assignee.txt' INTO TABLE assignee FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/inventor.txt' INTO TABLE inventor FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/ipcr.txt' INTO TABLE ipcr FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/lawyer.txt' INTO TABLE lawyer FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/usreldoc.txt' INTO TABLE usreldoc FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/uspc.txt' INTO TABLE uspc FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/citation.txt' INTO TABLE citation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '[dir]/otherreference.txt' INTO TABLE otherreference FIELDS TERMINATED by '\t' ENCLOSED BY '\"';

SET autocommit=1; 
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION tx_isolation='REPEATABLE-READ';