#!/usr/bin/env bash
if [ $# -ne 3 ]
  then
    echo "Usage: startup.sh <ENV> <AKKA_PORT> <HTTP_PORT>, ENV options: local, dev, qa"
    exit
fi
ENV=$1
AKKA_PORT=$2
HTTP_PORT=$3
cd `dirname $0`/../ansible
#rm target/${ENV}/*
echo "generating configuration files ......"
python genconf.py ${ENV}
cd ..
echo "launching application ......"
#sbt -Dlogback.configurationFile=./ansible/target/${ENV}/logback.xml "runMain com.vpon.ssp.report.dedup.Main -c ./ansible/target/${ENV}/application.conf"

sbt -DPORT=${AKKA_PORT} -Dlogback.configurationFile=./ansible/target/dev/logback.xml "runMain com.vpon.ssp.report.archive.Main -c ./ansible/target/dev/application.conf -h ${HTTP_PORT}"
