#configured to my starcluster environment

cd /mnt/sgeadmin
for i in `ls *.xml` 
    do echo $i 

    cd /home/sgeadmin/patentprocessor/
    python parse_sq.py -p /mnt/sgeadmin -xmlregex $i
    mysqldump -root uspto -T /var/lib/mysql/uspto
    mysql -root uspto -e "drop database uspto; create database uspto;"
    mysql [options] --local-infile=1 uspto < load.sql

done