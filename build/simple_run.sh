#!/bin/bash

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TAG=$1

ENV=$3

if [[ $2 ]]; then BUILD_NUMBER=$2; else BUILD_NUMBER="latest"; fi

PROJECT_NAME=`grep "val Name" ${BASE_DIR}/../project/build.scala | awk '{print $NF}' | awk -F \" '{print $2}'`

PROJECT_VERSION=`grep "val Version" ${BASE_DIR}/../project/build.scala | awk '{print $NF}' | awk -F \" '{print $2}'`

VERSION="v${PROJECT_VERSION}"

echo "version = ${VERSION}"

APP_NAME="${PROJECT_NAME}-${TAG}-${VERSION}"

ID=${APP_NAME}-${ENV}

AKKA_PORT=7661

HTTP_PORT=17661

LOGS_DIR=/data/logs/${PROJECT_NAME}

mkdir -p ${LOGS_DIR}

echo "${APP_NAME} started! AKKA port: ${AKKA_PORT}, HTTP port: ${HTTP_PORT}, log files located in: ${LOGS_DIR}"

sh ${BASE_DIR}/run.sh -g ${LOGS_DIR} -a ${AKKA_PORT} -p ${HTTP_PORT} -t ${TAG} -n ${BUILD_NUMBER} -v ${VERSION} -j "-Ddedup.cluster-group=${ID}" -e ${ENV}

exit 0
