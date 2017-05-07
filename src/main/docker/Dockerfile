FROM frolvlad/alpine-oraclejdk8:slim
VOLUME /tmp
VOLUME /data
VOLUME /parsed
VOLUME /root/.ssh
RUN apk update && apk upgrade
RUN apk add --no-cache bash git openssh
ADD daily-top-service-*.jar app.jar
RUN sh -c 'touch /app.jar'
ENV JAVA_OPTS="-Xmx6G"
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar" ]
