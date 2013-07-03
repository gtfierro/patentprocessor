/*
    Merge together two disctinct MySQL tables
    mysqldump [options] uspto -T /var/lib/mysql/uspto
*/

SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET SESSION tx_isolation='READ-UNCOMMITTED';
SET innodb_lock_wait_timeout = 500;
SET autocommit=0; 

LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/citation.txt' INTO TABLE citation FIELDS TERMINATED by '\t' ENCLOSED BY '\"';
LOAD DATA LOCAL INFILE '/var/lib/mysql/uspto/otherreference.txt' INTO TABLE otherreference FIELDS TERMINATED by '\t' ENCLOSED BY '\"';

SET autocommit=1; 
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION tx_isolation='REPEATABLE-READ';
SET innodb_lock_wait_timeout = 50;