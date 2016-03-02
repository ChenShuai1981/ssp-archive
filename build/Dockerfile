FROM jeanblanchard/java:8
MAINTAINER chenshuai <casel.chen@vpon.com>

RUN apk add --update bash
RUN mkdir /app

WORKDIR /

VOLUME ["/conf", "/logs"]

ADD *-assembly-*.jar /app/ssp-archive-assembly.jar

ENTRYPOINT ["java", "-server", "-Xmx2G", "-XX:MaxPermSize=100m", "-XX:+UseConcMarkSweepGC", "-XX:+DisableExplicitGC", "-Dlogback.configurationFile=/conf/logback.xml", "-DPORT=7661", "-cp", "/app/ssp-dedup-assembly.jar", "com.vpon.ssp.report.archive.Main", "-h", "17661", "-c", "/conf/application.conf"]
