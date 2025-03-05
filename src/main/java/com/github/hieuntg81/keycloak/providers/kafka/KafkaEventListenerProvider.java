package com.github.hieuntg81.keycloak.providers.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaEventListenerProvider implements EventListenerProvider {

  private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

  private final String topicEvents;

  private final List<EventType> events;

  private final String topicAdminEvents;

  private final Producer<String, String> producer;

  private final ObjectMapper mapper;

  public KafkaEventListenerProvider(String bootstrapServers, String clientId, String topicEvents, String[] events,
      String topicAdminEvents, Map<String, Object> kafkaProducerProperties, KafkaProducerFactory factory) {
    this.topicEvents = topicEvents;
    this.events = new ArrayList<>();
    this.topicAdminEvents = topicAdminEvents;

    for (String event : events) {
      try {
        EventType eventType = EventType.valueOf(event.toUpperCase());
        this.events.add(eventType);
      } catch (IllegalArgumentException e) {
        LOG.debugf("Ignoring event >%s<. Event does not exist.", event);
      }
    }

    producer = factory.createProducer(clientId, bootstrapServers, kafkaProducerProperties);
    mapper = new ObjectMapper();
  }

  private void produceEvent(String eventAsString, String topic) {
    LOG.debugf("Produce to topic: %s ...", topic);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventAsString);

    // Use virtual thread to send Kafka message asynchronously
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      executor.submit(() -> {
        try {
          var metaDataFuture = producer.send(record);
          RecordMetadata recordMetadata = metaDataFuture.get(30, TimeUnit.SECONDS);
          LOG.debugf("Produced to topic: %s, partition: %s, offset: %s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException e) {
          LOG.errorf("Interrupted while sending event to Kafka topic %s: %s", topic, e.getMessage(), e);
          Thread.currentThread().interrupt(); // Re-interrupt the thread
        } catch (ExecutionException e) {
          LOG.errorf("Error sending event to Kafka topic %s: %s", topic, e.getMessage(), e);
        } catch (TimeoutException e) {
          LOG.warnf("Timeout sending event to Kafka topic %s", topic);
        } catch (Exception e) {
          LOG.errorf("Unexpected error during Kafka send to topic %s: %s", topic, e.getMessage(), e);
        }
      });
    } catch (Exception e) {
      LOG.errorf("Error submitting task to virtual thread executor: %s", e.getMessage(), e);
    }
  }

  @Override
  public void onEvent(Event event) {
    if (events.contains(event.getType())) {
      try {
        produceEvent(mapper.writeValueAsString(event), topicEvents);
      } catch (JsonProcessingException e) {
        LOG.errorf("Error serializing Keycloak event to JSON: %s", e.getMessage(), e);
      }
    }
  }

  @Override
  public void onEvent(AdminEvent event, boolean includeRepresentation) {
    if (topicAdminEvents != null) {
      try {
        produceEvent(mapper.writeValueAsString(event), topicAdminEvents);
      } catch (JsonProcessingException e) {
        LOG.errorf("Error serializing Keycloak admin event to JSON: %s", e.getMessage(), e);
      }
    }
  }

  @Override
  public void close() {
    // ignore
  }
}
