package com.github.hieuntg81.keycloak.providers.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.keycloak.Config.Scope;
import org.jboss.logging.Logger;

public class KafkaProducerConfig {

  private static final Logger LOG = Logger.getLogger(KafkaProducerConfig.class);

  // Config Docs: https://kafka.apache.org/documentation/#producerconfigs

  public static Map<String, Object> init(Scope scope) {
    Map<String, Object> propertyMap = new HashMap<>();
    Arrays.stream(KafkaProducerProperty.values())
        .forEach(property -> {
          String propertyName = property.getName();
          String envVariableName = "KAFKA_" + property.name();
          String envValue = System.getenv(envVariableName);
          String configValue = scope.get(propertyName, envValue);

          if (configValue != null) {
            if (property == KafkaProducerProperty.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM &&
                "disabled".equalsIgnoreCase(configValue)) {
              propertyMap.put(propertyName, "");
              LOG.debugf("Setting Kafka producer property '%s' to empty string due to 'disabled' value.", propertyName);
            } else {
              propertyMap.put(propertyName, configValue);
              LOG.debugf("Setting Kafka producer property '%s' to '%s'.", propertyName, configValue);
            }
          } else {
            LOG.tracef("Kafka producer property '%s' not configured (neither in Keycloak config nor environment variable '%s').", propertyName, envVariableName);
          }
        });

    LOG.debugf("Initialized Kafka producer properties: %s", propertyMap);
    return propertyMap;
  }

  /**
   * Enum representing Kafka Producer properties.
   * Each enum value has a name corresponding to the Kafka producer configuration.
   *
   * @see <a href="https://kafka.apache.org/documentation/#producerconfigs">Kafka Producer Configurations</a>
   */
  enum KafkaProducerProperty {
    ACKS("acks"),
    BUFFER_MEMORY("buffer.memory"),
    COMPRESSION_TYPE("compression.type"),
    RETRIES("retries"),
    SSL_KEY_PASSWORD("ssl.key.password"),
    SSL_KEYSTORE_CERTIFICATE_CHAIN("ssl.keystore.certificate.chain"),
    SSL_KEYSTORE_LOCATION("ssl.keystore.location"),
    SSL_KEYSTORE_PASSWORD("ssl.keystore.password"),
    SSL_TRUSTSTORE_LOCATION("ssl.truststore.location"),
    SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password"),
    BATCH_SIZE("batch.size"),
    CLIENT_DNS_LOOKUP("client.dns.lookup"),
    CONNECTION_MAX_IDLE_MS("connections.max.idle.ms"),
    DELIVERY_TIMEOUT_MS("delivery.timeout.ms"),
    LINGER_MS("linger.ms"),
    MAX_BLOCK_MS("max.block.ms"),
    MAX_REQUEST_SIZE("max.request.size"),
    PARTITIONER_CLASS("partitioner.class"),
    RECEIVE_BUFFER_BYTES("receive.buffer.bytes"),
    REQUEST_TIMEOUT_MS("request.timeout.ms"),
    SASL_CLIENT_CALLBACK_HANDLER_CLASS("sasl.client.callback.handler.class"),
    SASL_JAAS_CONFIG("sasl.jaas.config"),
    SASL_KERBEROS_SERVICE_NAME("sasl.kerberos.service.name"),
    SASL_LOGIN_CALLBACK_HANDLER_CLASS("sasl.login.callback.handler.class"),
    SASL_LOGIN_CLASS("sasl.login.class"),
    SASL_MECHANISM("sasl.mechanism"),
    SECURITY_PROTOCOL("security.protocol"),
    SEND_BUFFER_BYTES("send.buffer.bytes"),
    SSL_ENABLED_PROTOCOLS("ssl.enabled.protocols"),
    SSL_KEYSTORE_TYPE("ssl.keystore.type"),
    SSL_PROTOCOL("ssl.protocol"),
    SSL_PROVIDER("ssl.provider"),
    SSL_TRUSTSTORE_TYPE("ssl.truststore.type"),
    ENABLE_IDEMPOTENCE("enable.idempotence"),
    INTERCEPTOR_CLASS("interceptor.classes"),
    MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION("max.in.flight.requests.per.connection"),
    METADATA_MAX_AGE_MS("metadata.max.age.ms"),
    METADATA_MAX_IDLE_MS("metadata.max.idle.ms"),
    METRIC_REPORTERS("metric.reporters"),
    METRIC_NUM_SAMPLES("metrics.num.samples"),
    METRICS_RECORDING_LEVEL("metrics.recording.level"),
    METRICS_SAMPLE_WINDOW_MS("metrics.sample.window.ms"),
    RECONNECT_BACKOFF_MAX_MS("reconnect.backoff.max.ms"),
    RECONNECT_BACKOFF_MS("reconnect.backoff.ms"),
    RETRY_BACKOFF_MS("retry.backoff.ms"),
    SASL_KERBEROS_KINIT_CMD("sasl.kerberos.kinit.cmd"),
    SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN("sasl.kerberos.min.time.before.relogin"),
    SASL_KERBEROS_TICKET_RENEW_JITTER("sasl.kerberos.ticket.renew.jitter"),
    SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR("sasl.kerberos.ticket.renew.window.factor"),
    SASL_LOGIN_REFRESH_BUFFER_SECONDS("sasl.login.refresh.buffer.seconds"),
    SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS("sasl.login.refresh.min.period.seconds"),
    SASL_LOGIN_REFRESH_WINDOW_FACTOR("sasl.login.refresh.window.factor"),
    SASL_LOGIN_REFRESH_WINDOW_JITTER("sasl.login.refresh.window.jitter"),
    SECURITY_PROVIDERS("security.providers"),
    SSL_CIPHER_SUITES("ssl.cipher.suites"),
    SSL_ENDPOINT_IDENTIFICATION_ALGORITHM("ssl.endpoint.identification.algorithm"),
    SSL_KEYMANAGER_ALGORITHM("ssl.keymanager.algorithm"),
    SSL_SECURE_RANDOM_IMPLEMENTATION("ssl.secure.random.implementation"),
    SSL_TRUSTMANAGER_ALGORITHM("ssl.trustmanager.algorithm"),
    TRANSACTION_TIMEOUT_MS("transaction.timeout.ms"),
    TRANSACTION_ID("transactional.id");

    private final String name;

    KafkaProducerProperty(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
