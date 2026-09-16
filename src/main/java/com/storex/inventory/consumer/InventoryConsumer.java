package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderEvent;
import com.storex.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

// Consumer lang nghe va xu ly su kien don hang tu topic order-events
@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    // Lang nghe tren topic order-events
    // Su dung containerFactory mac dinh da duoc cau hinh ErrorHandler, Retry va DLQ
    @KafkaListener(topics = "${app.kafka.topic.order-events:order-events}", groupId = "inventory-group")
    public void consume(@Payload OrderEvent event,
                        @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
                        @Header(name = KafkaHeaders.OFFSET, required = false) Long offset) {

        log.info("Processing order event: orderId={}, productId={}, quantity={}, partition={}, offset={}",
                event.getOrderId(), event.getProductId(), event.getQuantity(), partition, offset);

        // Goi service thuc hien tru so luong ton kho
        inventoryService.deductStock(event.getProductId(), event.getQuantity());

        log.info("Completed processing order event successfully for orderId: {}", event.getOrderId());
    }
}
