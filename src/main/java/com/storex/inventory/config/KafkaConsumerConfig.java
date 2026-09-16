package com.storex.inventory.config;

import com.storex.inventory.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

// Cau hinh Kafka Consumer, Producer, ErrorHandler, Retry va DLQ
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${app.kafka.topic.order-events-dlq:order-events.DLT}")
    private String dlqTopic;

    @Value("${app.kafka.retry.max-attempts:3}")
    private long maxRetryAttempts;

    @Value("${app.kafka.retry.interval-ms:1000}")
    private long retryIntervalMs;

    // ProducerFactory cho OrderEvent va JSON Object
    @Bean
    public ProducerFactory<Object, Object> jsonProducerFactory(KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties(null);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // KafkaTemplate cho JSON Object
    @Bean
    public KafkaTemplate<Object, Object> jsonKafkaTemplate(ProducerFactory<Object, Object> jsonProducerFactory) {
        return new KafkaTemplate<>(jsonProducerFactory);
    }

    // ProducerFactory cho du lieu tho dang byte array (phuc vu ghi DLQ khi deserialize loi)
    @Bean
    public ProducerFactory<Object, byte[]> byteProducerFactory(KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties(null);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // KafkaTemplate cho Byte Array
    @Bean
    public KafkaTemplate<Object, byte[]> byteKafkaTemplate(ProducerFactory<Object, byte[]> byteProducerFactory) {
        return new KafkaTemplate<>(byteProducerFactory);
    }

    // Recoverer day message loi sang Dead Letter Queue
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<Object, Object> jsonKafkaTemplate,
            KafkaTemplate<Object, byte[]> byteKafkaTemplate) {

        // Dang ky template tuong ung voi kieu payload
        Map<Class<?>, org.springframework.kafka.core.KafkaOperations<? extends Object, ? extends Object>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, byteKafkaTemplate);
        templates.put(Object.class, jsonKafkaTemplate);

        return new DeadLetterPublishingRecoverer(templates, (record, exception) -> {
            log.warn("Routing failed message to DLQ topic: {}. Key: {}, Partition: {}, Offset: {}, Reason: {}",
                    dlqTopic, record.key(), record.partition(), record.offset(), exception.getMessage());
            return new TopicPartition(dlqTopic, -1);
        });
    }

    // ErrorHandler voi co che Retry va Fallback sang DLQ
    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        // Cau hinh thu lai voi khoang thoi gian va so lan thu toi da
        FixedBackOff backOff = new FixedBackOff(retryIntervalMs, maxRetryAttempts);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Lang nghe va ghi log chi tiet moi lan retry
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("Retry attempt #{} failed for topic: {}, partition: {}, offset: {}. Error: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage());
        });

        return errorHandler;
    }

    // ConsumerFactory cho topic order-events su dung ErrorHandlingDeserializer
    @Bean
    public ConsumerFactory<String, OrderEvent> orderEventConsumerFactory(KafkaProperties properties) {
        Map<String, Object> props = properties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ContainerFactory cho main consumer
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> orderEventConsumerFactory,
            DefaultErrorHandler defaultErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderEventConsumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }

    // ConsumerFactory cho DLQ consumer
    @Bean
    public ConsumerFactory<String, String> dlqConsumerFactory(KafkaProperties properties) {
        Map<String, Object> props = properties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ContainerFactory danh rieng cho DLQ consumer
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dlqKafkaListenerContainerFactory(
            ConsumerFactory<String, String> dlqConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dlqConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }
}
