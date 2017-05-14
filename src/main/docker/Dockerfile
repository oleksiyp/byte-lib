FROM java:8-jdk-alpine
RUN apk update && apk upgrade && apk add --no-cache bash git openssh
ADD daily-top-service-*.jar app.jar
RUN sh -c 'touch /app.jar'
ENV JAVA_OPTS="-Xmx10G"
VOLUME /tmp
VOLUME /data
VOLUME /parsed
VOLUME /root/.ssh
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar" ]
