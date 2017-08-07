FROM freejackin/jdk8-python
RUN pip install pymongo requests kafka-python
VOLUME /tmp
ADD ./resources/application.yml application.yml
ADD ./resources/log4j2.properties log4j2.properties
ADD embedded2Mongo.jar app.jar
ADD rdm.py rdm.py
RUN sh -c 'touch /app.jar'
ENV JAVA_OPTS="-Dspring.config.location=file:/application.yml"
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar > extra.log | python -u rdm.py > rdm.log" ]
