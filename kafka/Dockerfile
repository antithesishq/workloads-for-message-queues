FROM docker.io/library/eclipse-temurin:21-jre-noble

RUN userdel -r ubuntu \
&& useradd -u 1000 -m -s /bin/bash appuser \
&& mkdir -p /var/lib/kafka/data /etc/kafka/secrets /usr/logs /mnt/shared/config \
&& chown appuser:appuser -R /usr/logs /mnt/shared/config \
&& chown appuser:root -R /var/lib/kafka /etc/kafka \
&& chmod -R ug+w /etc/kafka /var/lib/kafka

COPY --from=docker.io/apache/kafka:4.3.1 --chown=appuser:appuser /opt/kafka /opt/kafka
COPY --from=docker.io/apache/kafka:4.3.1 --chown=appuser:root /etc/kafka/docker /etc/kafka/docker

RUN mkdir -p /opt/kafka/instrumented-libs /opt/antithesis/catalog \
&& cd /opt/kafka/libs \
&& mv kafka_2.13-*.jar kafka-clients-*.jar kafka-coordinator-common-*.jar \
      kafka-group-coordinator-*.jar kafka-metadata-*.jar kafka-raft-*.jar \
      kafka-server-*.jar kafka-share-coordinator-*.jar kafka-storage-*.jar \
      kafka-transaction-coordinator-*.jar \
      /opt/kafka/instrumented-libs/ \
&& chown -R appuser:appuser /opt/kafka/instrumented-libs \
&& ln -s /opt/kafka/instrumented-libs /opt/antithesis/catalog/kafka

USER appuser
VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data", "/mnt/shared/config"]
EXPOSE 9092
ENV CLASSPATH="/opt/kafka/instrumented-libs/*"
ENV NO_COLOR=1
CMD ["/etc/kafka/docker/run"]