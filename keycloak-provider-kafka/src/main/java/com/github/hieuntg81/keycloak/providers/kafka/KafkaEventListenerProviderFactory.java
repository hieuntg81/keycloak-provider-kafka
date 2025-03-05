package com.github.hieuntg81.keycloak.providers.kafka;

import java.util.Arrays;
import java.util.Map;

import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

  private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);
  private static final String ID = "kafka";

  private KafkaEventListenerProvider instance;

  private String bootstrapServers;
  private String topicEvents;
  private String topicAdminEvents;
  private String clientId;
  private String[] events;
  private Map<String, Object> kafkaProducerProperties;

  @Override
  public EventListenerProvider create(KeycloakSession session) {
    if (instance == null) {
      instance = new KafkaEventListenerProvider(
          bootstrapServers,
          clientId,
          topicEvents,
          events,
          topicAdminEvents,
          kafkaProducerProperties,
          new KafkaStandardProducerFactory()
      );
    }
    return instance;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void init(Scope config) {
    LOG.infof("Initializing Kafka event listener module...");

    topicEvents = config.get("topicEvents", System.getenv("KAFKA_TOPIC"));
    clientId = config.get("clientId", System.getenv("KAFKA_CLIENT_ID"));
    bootstrapServers = config.get("bootstrapServers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
    topicAdminEvents = config.get("topicAdminEvents", System.getenv("KAFKA_ADMIN_TOPIC"));

    String eventsString = config.get("events", System.getenv("KAFKA_EVENTS"));

    if (eventsString != null) {
      events = eventsString.split(",");
    }

    // Validation - Using Objects.requireNonNull for concise null checks and clearer exceptions
    if (topicEvents == null) {
      throw new NullPointerException("Configuration 'topicEvents' (or environment variable KAFKA_TOPIC) must not be null.");
    }
    if (clientId == null) {
      throw new NullPointerException("Configuration 'clientId' (or environment variable KAFKA_CLIENT_ID) must not be null.");
    }
    if (bootstrapServers == null) {
      throw new NullPointerException("Configuration 'bootstrapServers' (or environment variable KAFKA_BOOTSTRAP_SERVERS) must not be null.");
    }

    if (events == null || events.length == 0) {
      LOG.warnf("No events configured. Defaulting to 'REGISTER' event.");
      events = new String[]{"REGISTER"}; // More concise array initialization
    } else {
      LOG.debugf("Configured event types: %s", Arrays.toString(events));
    }
    LOG.debugf("Kafka Bootstrap Servers: %s", bootstrapServers);
    LOG.debugf("Kafka Client ID: %s", clientId);
    LOG.debugf("Kafka Events Topic: %s", topicEvents);
    LOG.debugf("Kafka Admin Events Topic: %s", topicAdminEvents);


    kafkaProducerProperties = KafkaProducerConfig.init(config);
    LOG.debugf("Kafka Producer Properties: %s", kafkaProducerProperties);
  }

  @Override
  public void postInit(KeycloakSessionFactory factory) {
    // No post-initialization needed in this factory
    LOG.debug("Post-initialization hook called (no action needed).");
  }

  @Override
  public void close() {
    // Instance is a singleton and managed by Keycloak lifecycle.
    // No explicit resource release needed for the Factory itself in this simple case.
    LOG.debug("Close hook called (no action needed for factory).");
  }
}
