package com.storex.inventory.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// Service xu ly nghiep vu kho hang StoreX
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    // Luu tru so luong ton kho trong bo nho
    private final Map<String, Integer> stockRepository = new ConcurrentHashMap<>();

    // Dem so lan goi de phuc vu kiem thu retry
    private final AtomicInteger executionCount = new AtomicInteger(0);

    public InventoryService() {
        // Khoi tao ton kho mac dinh cho cac san pham
        stockRepository.put("PROD-001", 100);
        stockRepository.put("PROD-002", 50);
        stockRepository.put("PROD-ERROR", 10);
    }

    // Phuong thuc tru ton kho khi co don hang moi
    public void deductStock(String productId, Integer quantity) {
        int currentAttempt = executionCount.incrementAndGet();
        log.info("Deducting stock for productId: {}, quantity: {}, execution attempt: {}", productId, quantity, currentAttempt);

        if (productId == null || productId.trim().isEmpty()) {
            throw new IllegalArgumentException("ProductId cannot be null or empty");
        }

        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }

        // Gia lap truong hop he thong gap loi de kiem thu co che retry
        if ("PROD-ERROR".equals(productId)) {
            log.error("Simulated business or database error occurred for productId: {}", productId);
            throw new RuntimeException("Database connection timeout while deducting stock");
        }

        int currentStock = stockRepository.getOrDefault(productId, 0);
        if (currentStock < quantity) {
            throw new IllegalStateException("Insufficient stock for productId: " + productId);
        }

        stockRepository.put(productId, currentStock - quantity);
        log.info("Stock deducted successfully for productId: {}. Remaining stock: {}", productId, stockRepository.get(productId));
    }

    public int getStock(String productId) {
        return stockRepository.getOrDefault(productId, 0);
    }

    public void setStock(String productId, int quantity) {
        stockRepository.put(productId, quantity);
    }

    public int getExecutionCount() {
        return executionCount.get();
    }

    public void resetExecutionCount() {
        executionCount.set(0);
    }
}
