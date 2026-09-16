package com.storex.inventory.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// Consumer giam sat va xu ly cac tin nhan bi loi chuyen vao Dead Letter Queue
@Service
public class InventoryDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryDlqConsumer.class);

    // Danh sach luu tru cac ban tin DLQ nhan duoc phuc vu kiem tra va debug
    private final List<ConsumerRecord<String, String>> dlqRecords = new CopyOnWriteArrayList<>();

    // Lang nghe topic DLQ su dung container factory danh rieng cho String
    @KafkaListener(
            topics = "${app.kafka.topic.order-events-dlq:order-events.DLT}",
            groupId = "inventory-dlq-group",
            containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void consumeDlq(ConsumerRecord<String, String> record) {
        log.error("Received poison pill message in DLQ: topic={}, partition={}, offset={}, key={}, payload={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());

        // Ghi nhan cac header dac trung do DeadLetterPublishingRecoverer dinh kem
        record.headers().forEach(header -> {
            String key = header.key();
            if (key.startsWith("kafka_dlt")) {
                log.error("DLT Header [{}]: {}", key, new String(header.value()));
            }
        });

        dlqRecords.add(record);
    }

    public List<ConsumerRecord<String, String>> getDlqRecords() {
        return dlqRecords;
    }

    public void clearDlqRecords() {
        dlqRecords.clear();
    }
}
