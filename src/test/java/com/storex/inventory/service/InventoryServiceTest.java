package com.storex.inventory.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Kiem thu don vi cho InventoryService
public class InventoryServiceTest {

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService();
        inventoryService.setStock("PROD-001", 100);
    }

    @Test
    @DisplayName("Tru ton kho thanh cong khi du lieu hop le")
    void testDeductStock_Success() {
        inventoryService.deductStock("PROD-001", 20);
        assertEquals(80, inventoryService.getStock("PROD-001"));
    }

    @Test
    @DisplayName("Nem ngoai le khi productId rong hoac null")
    void testDeductStock_InvalidProductId() {
        assertThrows(IllegalArgumentException.class, () -> inventoryService.deductStock("", 10));
        assertThrows(IllegalArgumentException.class, () -> inventoryService.deductStock(null, 10));
    }

    @Test
    @DisplayName("Nem ngoai le khi quantity nho hon hoac bang 0")
    void testDeductStock_InvalidQuantity() {
        assertThrows(IllegalArgumentException.class, () -> inventoryService.deductStock("PROD-001", 0));
        assertThrows(IllegalArgumentException.class, () -> inventoryService.deductStock("PROD-001", -5));
    }

    @Test
    @DisplayName("Nem ngoai le khi so luong ton kho khong du")
    void testDeductStock_InsufficientStock() {
        assertThrows(IllegalStateException.class, () -> inventoryService.deductStock("PROD-001", 150));
    }

    @Test
    @DisplayName("Nem ngoai le khi gap san pham mo phong loi he thong")
    void testDeductStock_SimulatedSystemError() {
        assertThrows(RuntimeException.class, () -> inventoryService.deductStock("PROD-ERROR", 1));
    }
}
