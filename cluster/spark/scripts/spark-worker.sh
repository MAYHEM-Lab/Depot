#!/usr/bin/env bash

. $SPARK_HOME/sbin/spark-config.sh
. $SPARK_HOME/bin/load-spark-env.sh

export SPARK_LOGS=$SPARK_HOME/logs

mkdir $SPARK_LOGS
ln -sf /dev/stdout $SPARK_LOGS/spark-worker.log

spark-class org.apache.spark.deploy.worker.Worker $1 >> $SPARK_LOGS/spark-worker.log
