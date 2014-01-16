#!/bin/bash

dbtype=`awk -F " = " '/database/ {print $2}' lib/alchemy/config.ini | head -n 1`

cd lib
if [ $dbtype == "sqlite" ] ; then
  celery -A tasks worker --loglevel=info --logfile=celery.log --concurrency=1 &
  celeryPID=$!
else
  celery -A tasks worker --loglevel=info --logfile=celery.log --concurrency=3 &
  celeryPID=$!
fi
redis-server &
redisPID=$!
cd ..
echo $celeryPID > celery.pid
echo $redisPID > redis.pid

echo 'Running assignee disambiguation'
python lib/assignee_disambiguation.py $1

if [ $1 == "grant" ] ; then
  echo 'Running lawyer disambiguation'
  python lib/lawyer_disambiguation.py $1
fi

echo 'Running geo disambiguation'
python lib/geoalchemy.py $1

kill $celeryPID  # kill celery
kill $redisPID # kill redis
rm lib/dump.rdb # remove redis dump
# remove pid files
rm celery.pid redis.pid
