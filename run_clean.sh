#!/bin/bash

cd lib
celery -A tasks worker --loglevel=info --logfile=celery.log --concurrency=3 &
celeryPID=$!
redis-server &
redisPID=$!
cd ..
echo $celeryPID > celery.pid
echo $redisPID > redis.pid

python lib/assignee_disambiguation.py $1
#echo 'Running assignee disambiguation'
#for i in {a..z} ; do
#    python lib/assignee_disambiguation.py $1 $i
#    sleep 2
#done

kill $celeryPID  # kill celery
kill $redisPID # kill redis
rm lib/dump.rdb # remove redis dump
# remove pid files
rm celery.pid redis.pid

if [ $1 = "grant" ]
    then
        echo 'Running lawyer disambiguation'
        python lib/lawyer_disambiguation.py
        #for i in {a..z} ; do
        #done
fi

echo 'Running geo disambiguation'
python lib/geoalchemy.py $1
