package com.github.hieuntg81.keycloak.providers.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Producer;

interface KafkaProducerFactory {

  Producer<String, String> createProducer(String clientId, String bootstrapServer,
      Map<String, Object> optionalProperties);

}
