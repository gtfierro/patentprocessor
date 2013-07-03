#configured to my starcluster environment

cd /mnt/sgeadmin
for i in `ls *.xml`
    do echo $i

    cd /home/sgeadmin/patentprocessor/
    echo " - python"
    python parse_sq.py -p /mnt/sgeadmin --xmlregex $i
    echo " - mysqldump"
    mysqldump -root uspto -T /var/lib/mysql/uspto
    echo " - drop database"
    mysql -root uspto < starcluster/load_drop.sql
    echo " - ingest"
    mysql [options] --local-infile=1 uspto < starcluster/load_easy.sql
    echo " - duplicate"
    cd /var/lib/mysql/uspto
    cp patent.txt patent_$i.txt
    cp application.txt application_$i.txt
    cp location.txt location_$i.txt
    cp assignee.txt assignee_$i.txt
    cp inventor.txt inventor_$i.txt
    cp lawyer.txt lawyer_$i.txt
    cp usreldoc.txt usreldoc_$i.txt
    cp uspc.txt uspc_$i.txt
    cp ipcr.txt ipcr_$i.txt
    cp mainclass.txt mainclass_$i.txt
    cp subclass.txt subclass_$i.txt

    cp citation.txt citation_$i.txt
    cp otherreference.txt citation_$i.txt
    tar -czf $i.tar.gz *_$i.txt
    rm *_$i.txt
    mv $i.tar.gz /mnt/sgeadmin

done