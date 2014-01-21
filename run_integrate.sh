#!/bin/bash

dbtype=`awk -F " = " '/database/ {print $2}' lib/alchemy/config.ini | head -n 1`
#cd lib
if [ $dbtype == "sqlite" ] ; then
  celery -A lib.tasks worker --loglevel=info --logfile=celery.log --concurrency=1 &
  celeryPID=$!
else
  celery -A lib.tasks worker --loglevel=info --logfile=celery.log --concurrency=3 &
  celeryPID=$!
fi
redis-server &
redisPID=$!
#cd ..
echo $celeryPID > celery.pid
echo $redisPID > redis.pid

python integrate.py $1 $2

kill $celeryPID  # kill celery
kill $redisPID # kill redis
rm lib/dump.rdb # remove redis dump
# remove pid files
rm celery.pid redis.pid
