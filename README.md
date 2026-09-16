# Session 10 - Bài tập 4: Xử lý lỗi cho Kafka Consumer với Retry và Dead Letter Queue (DLQ)

## Giới thiệu
Dự án triển khai giải pháp xử lý lỗi chuyên sâu cho Kafka Consumer trong hệ thống quản lý kho StoreX bằng Spring Boot 3 và Spring Kafka, ngăn chặn triệt để sự cố kẹt Consumer (Poison Pill / Head-of-Line Blocking) khi gặp chuỗi JSON sai định dạng hoặc ngoại lệ nghiệp vụ.

## Cấu trúc thư mục dự án
```text
Session10_Bai4/
├── build.gradle
├── settings.gradle
├── gradlew
├── gradlew.bat
├── .gitignore
├── README.md
├── BAO_CAO_PHAN_TICH.md
└── src/
    ├── main/
    │   ├── java/com/storex/inventory/
    │   │   ├── InventoryApplication.java
    │   │   ├── config/
    │   │   │   └── KafkaConsumerConfig.java
    │   │   ├── consumer/
    │   │   │   ├── InventoryConsumer.java
    │   │   │   └── InventoryDlqConsumer.java
    │   │   ├── model/
    │   │   │   └── OrderEvent.java
    │   │   └── service/
    │   │       └── InventoryService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/storex/inventory/
            ├── consumer/
            │   └── InventoryConsumerIntegrationTest.java
            └── service/
                └── InventoryServiceTest.java
```

## Các tính năng chính
1. **ErrorHandlingDeserializer**: Bắt lỗi JSON sai định dạng ngay tại tầng deserialization, không để exception làm sập poll loop.
2. **DefaultErrorHandler**: Cấu hình Retry tối đa 3 lần với khoảng cách 1000ms (sử dụng FixedBackOff).
3. **DeadLetterPublishingRecoverer**: Tự động chuyển tiếp tin nhắn hỏng sang topic DLQ (`order-events.DLT`) kèm theo metadata và nguyên nhân lỗi trên Header.
4. **Commit Offset an toàn**: Sau khi đẩy vào DLQ, commit offset của bản tin hỏng để giải phóng Consumer đọc tiếp các tin nhắn hợp lệ phía sau.
5. **InventoryDlqConsumer**: Lắng nghe và ghi log giám sát các bản tin trong Dead Letter Queue.

## Hướng dẫn chạy kiểm thử
Dự án sử dụng Gradle và Embedded Kafka, có thể chạy trực tiếp mà không cần cài đặt Kafka Broker bên ngoài:

```bash
# Chay toan bo cac test case
./gradlew test

# Chay test kem log chi tiet
./gradlew test --info
```

## Tài liệu phân tích chuyên sâu
Xem chi tiết nguyên nhân gốc rễ, cơ chế quản lý Offset, so sánh kiến trúc tại file [BAO_CAO_PHAN_TICH.md](BAO_CAO_PHAN_TICH.md).
