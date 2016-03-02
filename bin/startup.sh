#!/usr/bin/env bash
if [ $# -eq 0 ]
  then
    echo "Usage: startup.sh <ENV>, ENV options: local, dev, qa"
    exit
fi
ENV=$1
cd `dirname $0`/../ansible
#rm target/${ENV}/*
echo "generating configuration files ......"
python genconf.py ${ENV}
cd ..
echo "launching application ......"
sbt -Dlogback.configurationFile=./ansible/target/${ENV}/logback.xml "runMain com.vpon.ssp.report.archive.Main -c ./ansible/target/${ENV}/application.conf"
