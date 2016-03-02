#!/bin/bash

SHORT=hp:c:g:t:n:j:o:v:a:e:
LONG=help,conf-dir:,logs-dir:,http-port:,tag:,build-number:,jvm:,options:,version:,environment:,akka-port:

PARSED=`getopt --options $SHORT --longoptions $LONG --name "$0" -- "$@"`
if [[ $? != 0 ]]; then
    exit 2
fi

eval set -- "$PARSED"

while true; do
    case "$1" in
        -h|--help)
            NEED_HELP=y
            shift
            ;;
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -t|--tag)
            TAG="$2"
            shift 2
            ;;
        -j|--jvm)
            JVM_OPT="$2"
            shift 2
            ;;
        -o|--options)
            OPTS="$2"
            shift 2
            ;;
        -n|--build-number)
            BUILD_NUMBER="$2"
            shift 2
            ;;
        -p|--http-port)
            HTTP_PORT="$2"
            shift 2
            ;;
        -c|--conf-dir)
            CONF_DIR="$2"
            shift 2
            ;;
        -g|--logs-dir)
            LOGS_DIR="$2"
            shift 2
            ;;
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -a|--akka-port)
            AKKA_PORT="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Command line parameters parsing error!"
            echo "exit!"
            exit 3
            ;;
    esac
done

function help() {
    echo "
usage:
-t, --tag             required    docker image tag, eg: release, dev, feature
-n, --build-number    required    docker image build number, eg: latest, 123
-c, --conf-dir        optional    conf dir
-g, --logs-dir        required    logs dir
-a, --akka-port       required    akka port, eg: 7661
-p, --http-port       required    http port, eg: 17661
-j, --jvm             optional    JVM OPTS, eg: \"-Xmx8192m -Dfile.encoding=UTF-8\"
-o, --options         optional    other application options, eg: \"--name ssp-reporting-service -h 10.0.1.2\", run \"sbt 'run --help'\" to get details
-e, --environment     required    environment, eg: dev, qa
-h, --help            required    print this help message

"
}

if [[ ${NEED_HELP} ]]; then
    help
    exit 0
fi

if [[ ! $TAG || ! $BUILD_NUMBER || ! $HTTP_PORT || ! $LOGS_DIR || ! $VERSION || ! $AKKA_PORT || ! $ENVIRONMENT ]]; then
    echo ""
    echo "Error! parameters missing!"
    help
    exit 1
fi

APP_NAME="ssp-dedup-${TAG}-${VERSION}"
IMAGE="192.168.105.243:5000/vpon/${APP_NAME}:${BUILD_NUMBER}"

if [[ ${CONF_DIR} ]]; then
    CONF_MOUNT="-v ${CONF_DIR}:/conf"
fi

mkdir -p ${LOGS_DIR}

docker ps -a | grep -e "[[:space:]]${APP_NAME}[[:space:]]" && docker rm -f ${APP_NAME} && docker rmi -f ${IMAGE} > /dev/null

docker pull ${IMAGE}

ENTRYPOINT="java ${JVM_OPT} -Dlogback.configurationFile=/opt/apps/conf/${ENVIRONMENT}/logback.xml -DPORT=${AKKA_PORT} -classpath /opt/apps/ssp-dedup-assembly.jar com.vpon.ssp.report.dedup.Main -h ${HTTP_PORT} -c /opt/apps/conf/${ENVIRONMENT}/application.conf ${OPTS}"

docker run -itd --privileged=true --name ${APP_NAME} --net=host ${CONF_MOUNT} -v ${LOGS_DIR}:/logs ${IMAGE} ${ENTRYPOINT}

echo "${APP_NAME} started! AKKA port is ${AKKA_PORT}, HTTP port is ${HTTP_PORT}, log files located in ${LOGS_DIR}"

#docker attach `docker ps | grep ${APP_NAME} | awk '{print $1}'`
