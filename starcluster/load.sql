/*
    Merge together two disctinct MySQL tables
    mysqldump [options] uspto -T /var/lib/mysql/uspto
*/

SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET SESSION tx_isolation='READ-UNCOMMITTED';

LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/patent.txt' IGNORE INTO TABLE patent FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/location.txt' IGNORE INTO TABLE location FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/subclass.txt' IGNORE INTO TABLE subclass FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/mainclass.txt' IGNORE INTO TABLE mainclass FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/application.txt' IGNORE INTO TABLE application FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/assignee.txt' IGNORE INTO TABLE assignee FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/citation.txt' IGNORE INTO TABLE citation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/inventor.txt' IGNORE INTO TABLE inventor FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/ipcr.txt' IGNORE INTO TABLE ipcr FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/lawyer.txt' IGNORE INTO TABLE lawyer FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/otherreference.txt' IGNORE INTO TABLE otherreference FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/usreldoc.txt' IGNORE INTO TABLE usreldoc FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/uspc.txt' IGNORE INTO TABLE uspc FIELDS TERMINATED by '\t' ENCLOSED BY '\"';

SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION tx_isolation='REPEATABLE-READ';