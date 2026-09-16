package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderEvent;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Kiem thu tich hop xu ly loi Kafka Consumer voi Retry va DLQ
@SpringBootTest(properties = {
        "app.kafka.retry.interval-ms=200",
        "app.kafka.retry.max-attempts=3",
        "app.kafka.topic.order-events=order-events",
        "app.kafka.topic.order-events-dlq=order-events.DLT"
})
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-events", "order-events.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
public class InventoryConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Object, Object> jsonKafkaTemplate;

    @Autowired
    private InventoryConsumer inventoryConsumer;

    @Autowired
    private InventoryDlqConsumer inventoryDlqConsumer;

    @Autowired
    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryDlqConsumer.clearDlqRecords();
        inventoryService.resetExecutionCount();
        inventoryService.setStock("PROD-001", 100);
        inventoryService.setStock("PROD-002", 50);
        inventoryService.setStock("PROD-ERROR", 10);
    }

    // Test case 1: Message hop le duoc xu ly thanh cong ngay lan dau
    @Test
    @DisplayName("Kiem thu tin nhan hop le - tru ton kho thanh cong")
    void testValidMessage_ProcessedSuccessfully() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-101")
                .productId("PROD-001")
                .quantity(10)
                .timestamp(System.currentTimeMillis())
                .build();

        jsonKafkaTemplate.send("order-events", event.getOrderId(), event);

        // Kiem tra ton kho duoc tru tu 100 xuong 90
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(90, inventoryService.getStock("PROD-001"));
            assertEquals(1, inventoryService.getExecutionCount());
            assertTrue(inventoryDlqConsumer.getDlqRecords().isEmpty());
        });
    }

    // Test case 2: Loi nghiep vu duoc retry toi da 3 lan roi chuyen vao DLQ
    @Test
    @DisplayName("Kiem thu loi nghiep vu - retry 3 lan va day vao DLQ")
    void testBusinessError_RetriesThreeTimesAndRoutesToDlq() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-ERR-001")
                .productId("PROD-ERROR")
                .quantity(2)
                .timestamp(System.currentTimeMillis())
                .build();

        jsonKafkaTemplate.send("order-events", event.getOrderId(), event);

        // Cho thuc hien 1 lan ban dau + 3 lan retry = 4 lan thuc thi
        // Sau do message duoc day vao DLQ
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(4, inventoryService.getExecutionCount());
            assertEquals(1, inventoryDlqConsumer.getDlqRecords().size());
            assertEquals("ORDER-ERR-001", inventoryDlqConsumer.getDlqRecords().get(0).key());
        });
    }

    // Test case 3: JSON loi dinh dang (Poison Pill) duoc day vao DLQ va khong lam ket consumer
    @Test
    @DisplayName("Kiem thu JSON sai dinh dang - day vao DLQ va khong gay ket tin nhan tiep theo")
    void testMalformedJson_SentToDlqAndConsumerNotStuck() {
        // Tao producer gui chuoi tho khong hop le
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Gui tin nhan JSON thieu dau dong ngoac nhon
        String malformedJson = "{\"orderId\":\"ORDER-MALFORMED\",\"productId\":\"PROD-001\",\"quantity\":5";
        try (Producer<String, String> producer = new DefaultKafkaProducerFactory<String, String>(senderProps).createProducer()) {
            producer.send(new ProducerRecord<>("order-events", "KEY-MALFORMED", malformedJson));
            producer.flush();
        }

        // Kiem tra message loi duoc chuyen sang DLQ boi ErrorHandlingDeserializer va ErrorHandler
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(inventoryDlqConsumer.getDlqRecords().isEmpty());
            assertEquals("KEY-MALFORMED", inventoryDlqConsumer.getDlqRecords().get(0).key());
        });

        // Gui tiep tin nhan hop le ngay phia sau de kiem chung Consumer khong he bi ket
        OrderEvent nextValidEvent = OrderEvent.builder()
                .orderId("ORDER-102")
                .productId("PROD-002")
                .quantity(15)
                .timestamp(System.currentTimeMillis())
                .build();

        jsonKafkaTemplate.send("order-events", nextValidEvent.getOrderId(), nextValidEvent);

        // Kiem tra tin nhan tiep theo van duoc xu ly thanh cong, ton kho giam tu 50 ve 35
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(35, inventoryService.getStock("PROD-002"));
        });
    }
}
