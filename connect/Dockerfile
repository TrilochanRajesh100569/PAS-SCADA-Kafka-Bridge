# Custom Kafka Connect image with Camel kafka-connector plugins.
#
# The Maven tarballs ship JARs flat at the connector-folder root (no lib/),
# so Connect's plugin scanner can't find SinkConnector/SourceConnector
# implementation classes. We extract, then move JARs into <connector>/lib/
# and the etc/ files into <connector>/etc/ — the layout Connect actually expects.
#
# Plugins installed:
#   1. camel-jms-pooled-apache-artemis-source  (TMS Artemis → Kafka)
#   2. camel-spring-rabbitmq-sink              (Kafka → RabbitMQ AMQP)
#   3. camel-mqtt5-sink                        (Kafka → MQTT)
#   4. camel-spring-rabbitmq-source            (RabbitMQ → Kafka, reverse)
#   5. camel-jms-pooled-apache-artemis-sink    (Kafka → Artemis, reverse)

FROM confluentinc/cp-kafka-connect:7.8.0

ARG CKC_VERSION=4.8.3
ARG MAVEN_BASE=https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector

USER root

RUN set -eux; \
    PLUGINS="camel-jms-pooled-apache-artemis-source-kafka-connector \
             camel-spring-rabbitmq-sink-kafka-connector \
             camel-mqtt5-sink-kafka-connector \
             camel-spring-rabbitmq-source-kafka-connector \
             camel-jms-pooled-apache-artemis-sink-kafka-connector"; \
    DEST=/usr/share/confluent-hub-components; \
    mkdir -p "$DEST"; \
    rm -rf "$DEST"/camel-* || true; \
    for p in $PLUGINS; do \
      url="${MAVEN_BASE}/${p}/${CKC_VERSION}/${p}-${CKC_VERSION}-package.tar.gz"; \
      echo "→ ${p}"; \
      curl -fsSL -o /tmp/plugin.tar.gz "${url}"; \
      tar -xzf /tmp/plugin.tar.gz -C "$DEST/" --exclude='docs'; \
      rm /tmp/plugin.tar.gz; \
      cd "$DEST/$p"; \
      mkdir -p lib etc; \
      for j in *.jar; do [ -f "$j" ] && mv "$j" lib/; done; \
      for f in *.properties; do [ -f "$f" ] && mv "$f" etc/; done; \
      echo "  $(ls lib/ | wc -l) JARs in lib/"; \
      cd /; \
    done; \
    echo "── Final layout ──"; \
    ls -la "$DEST/"

USER appuser

EXPOSE 8083
