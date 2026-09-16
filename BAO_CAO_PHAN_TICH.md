# BÁO CÁO PHÂN TÍCH VÀ THIẾT KẾ KIẾN TRÚC HỆ THỐNG
## BÀI TẬP 4: XỬ LÝ LỖI CHO KAFKA CONSUMER VỚI RETRY VÀ DEAD LETTER QUEUE (DLQ)

---

## 1. Bối cảnh bài toán và hiện trạng hệ thống

Hệ thống quản trị kho hàng của StoreX vận hành mô hình kiến trúc hướng sự kiện (Event-Driven Architecture) sử dụng Apache Kafka. Khi khách hàng đặt đơn trên sàn thương mại điện tử, Order Service sẽ xuất bản (publish) một sự kiện đơn hàng vào topic `order-events`. Phía kho hàng, `InventoryConsumer` thuộc consumer group `inventory-group` liên tục lắng nghe topic này để tự động tính toán và khấu trừ số lượng tồn kho sản phẩm (`deductStock`) theo thời gian thực.

Mã nguồn ban đầu của hệ thống:

```java
// InventoryConsumer.java -- CODE ĐANG CÓ VẤN ĐỀ
 
@Service
public class InventoryConsumer {
 
    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void consume(OrderEvent event) {
        // Nếu event bị lỗi JSON, exception sẽ throw và consumer bị kẹt
        inventoryService.deductStock(event.getProductId(), event.getQuantity());
    }
}
```

### Hiện trạng lỗi thực tế
Trong quá trình vận hành cao điểm, một số message xuất phát từ hệ thống đặt hàng gặp sự cố định dạng:
- Chuỗi JSON bị cắt cụt hoặc thiếu dấu ngoặc nhọn kết thúc `}` (Malformed JSON).
- Trường dữ liệu không đúng chuẩn hoặc payload rỗng.
- Hoặc trong quá trình thực thi nghiệp vụ trừ kho, database bị deadlock hoặc ném ngoại lệ RuntimeException không được kiểm soát.

Khi `InventoryConsumer` đọc phải bản tin lỗi này, ứng dụng văng ngoại lệ liên tục trong một vòng lặp vô tận. Toàn bộ các đơn hàng hợp lệ xếp hàng phía sau trong cùng partition bị phong tỏa hoàn toàn (Head-of-Line Blocking), kho hàng tê liệt và không thể cập nhật tồn kho cho khách hàng.

---

## 2. Phần 1: Phân tích nguyên nhân gốc rễ (Root Cause Analysis)

### 2.1. Cơ chế quản lý và đọc Offset của Apache Kafka

Apache Kafka không hoạt động theo mô hình message queue truyền thống (như ActiveMQ hay RabbitMQ vốn xóa tin nhắn ngay sau khi consumer đọc xong). Kafka tổ chức lưu trữ dữ liệu dưới dạng **Append-Only Commit Log** trên từng Partition:

```
Topic: order-events (Partition 0)
[Offset 0] -> [Offset 1] -> [Offset 2 (Lỗi)] -> [Offset 3] -> [Offset 4] -> [LEO]
                                    ^
                             Current Position
```

Các khái niệm then chốt về Offset trong Kafka:

1. **Log End Offset (LEO):**
   - Là vị trí offset của bản tin tiếp theo sẽ được ghi vào partition của topic.
2. **Current Offset (Position):**
   - Là con trỏ đọc hiện tại của Consumer Instance trên partition, được lưu trong bộ nhớ của tiến trình Consumer. Khi consumer gọi hàm `poll()`, Kafka client sẽ gửi request lấy các record bắt đầu từ `Current Position` này.
3. **Committed Offset:**
   - Là offset lớn nhất mà Consumer Group đã thông báo chính thức cho Kafka Broker rằng *"Các message trước offset này đã được xử lý xong"*.
   - Giá trị này được lưu trữ bền vững trong topic nội bộ đặc biệt của Kafka là `__consumer_offsets`.
   - Nếu Consumer bị crash, khởi động lại, hoặc xảy ra Rebalance, Kafka sẽ căn cứ vào `Committed Offset` này để giao việc tiếp tục đọc cho Consumer mới.
4. **Consumer Lag:**
   - Là khoảng chênh lệch giữa vị trí ghi mới nhất của Producer và vị trí đã commit của Consumer:
     $$\text{Consumer Lag} = \text{LEO} - \text{Committed Offset}$$
   - Lag càng lớn phản ánh hệ thống xử lý càng bị chậm trễ hoặc đang bị tắc nghẽn.

---

### 2.2. Lý do Consumer bị kẹt (Vòng lặp vô hạn Poison Pill & Head-of-Line Blocking)

Hiện tượng Consumer bị kẹt tại một tin nhắn xuất phát từ sự kết hợp của hai cơ chế: **Cơ chế Commit Offset** và **Cơ chế Quản lý Phân vùng Tuần tự**.

#### A. Phân loại hai tầng lỗi xảy ra:
1. **Lỗi Deserialization (Tầng hạ tầng):**
   - Chuỗi JSON gửi tới topic bị sai cú pháp (ví dụ thiếu `}`).
   - Bộ giải mã `JsonDeserializer` thực hiện chuyển đổi byte array nhận từ Broker sang đối tượng `OrderEvent` trước khi đưa vào hàm `consume()`.
   - Khi gặp JSON hỏng, `JsonDeserializer` ném trực tiếp `SerializationException` / `RecordDeserializationException` ngay trong vòng lặp `poll()`. Hàm `consume()` thậm chí chưa hề được gọi tới.
2. **Lỗi Runtime/Business Exception (Tầng ứng dụng):**
   - Dữ liệu JSON hợp lệ về mặt cú pháp nhưng vi phạm ràng buộc dữ liệu hoặc lỗi cơ sở dữ liệu khi gọi `inventoryService.deductStock()`.

#### B. Tại sao lại sinh ra vòng lặp kẹt vô tận?
- **Nguyên lý An toàn Dữ liệu (At-least-once delivery):**
  - Mặc định, Spring Kafka chỉ commit offset khi một record được xử lý trọn vẹn và phương thức listener trả về thành công mà không ném ra ngoại lệ.
  - Khi `consume()` ném ngoại lệ (hoặc Deserializer gặp lỗi), Spring Kafka hiểu rằng việc xử lý record đó đã thất bại.
  - Nhằm tránh mất mát dữ liệu (data loss), container lắng nghe sẽ:
    1. **Không commit offset** của record lỗi vào topic `__consumer_offsets`.
    2. Thực hiện thao tác `seek` (lùi con trỏ đọc) quay trở lại đúng vị trí offset của record lỗi vừa thất bại.
- **Hệ quả của vòng lặp:**
  - Ở chu kỳ `poll()` tiếp theo, Consumer gửi request tới Broker với vị trí bắt đầu là `Committed Offset` cũ (chính là offset của tin nhắn lỗi).
  - Consumer nhận lại y nguyên tin nhắn lỗi đó.
  - Quá trình giải mã hoặc xử lý lại ném ra ngoại lệ như cũ.
  - Container lại tiếp tục `seek` lùi offset và lặp lại liên tục hàng trăm lần mỗi giây.

#### C. Thảm họa Head-of-Line Blocking:
- Mỗi partition trong Kafka là một cấu trúc hàng đợi tuần tự nghiêm ngặt (FIFO per partition).
- Consumer không thể "nhảy cóc" qua một tin nhắn chưa được commit để đọc tin nhắn phía sau.
- Bản tin lỗi hoạt động như một **Viên thuốc độc (Poison Pill)**:
  - Bản thân nó không thể nuốt trôi.
  - Nó đứng chặn ngay đầu hàng đợi (`Head of Line`), khiến toàn bộ hàng nghìn tin nhắn hợp lệ xếp hàng phía sau bị dồn ứ (Lag tăng vọt).
  - Toàn bộ nghiệp vụ kho của StoreX bị đình trệ hoàn toàn.

---

## 3. Phần 2: Thiết kế giải pháp kiến trúc (Retry & Dead Letter Queue)

Để giải quyết triệt để sự cố mà không làm mất mát dữ liệu hay đình trệ dây chuyền, hệ thống áp dụng mô hình phối hợp 4 tầng xử lý:

```
[Kafka Broker: order-events]
            |
            v
[ErrorHandlingDeserializer] ---> (Nếu JSON hỏng) ---> [DefaultErrorHandler]
            | (Nếu JSON đúng)                                |
            v                                                |
   [InventoryConsumer]                                       |
            |                                                |
   [InventoryService]                                        |
            | (Nếu ném Business Exception)                   |
            +------------------------------------------------+
                                    |
                                    v
                         [Retry Policy: 3 lần]
                                    |
                    (Nếu thử lại 3 lần vẫn thất bại)
                                    |
                                    v
                     [DeadLetterPublishingRecoverer]
                                    |
                                    v
                    [Kafka Topic: order-events.DLT]
                                    |
                                    +---> [Commit Offset ban đầu an toàn]
                                    +---> [Consumer đọc tiếp tin tiếp theo]
```

---

### 3.1. Thành phần 1: ErrorHandlingDeserializer (Tầng Deserializer)
- Thay vì sử dụng `JsonDeserializer` trực tiếp làm value deserializer (dễ gây sập vòng lặp `poll()`), cấu hình `ErrorHandlingDeserializer` làm deserializer bọc ngoài (delegating wrapper).
- Khi gặp chuỗi JSON sai định dạng:
  - `ErrorHandlingDeserializer` bắt ngoại lệ `DeserializationException`.
  - Không để exception làm sập poll loop, mà đưa thông tin ngoại lệ vào Kafka Record Header (`SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER`).
  - Đặt giá trị payload trong ConsumerRecord là `null` hoặc raw bytes, chuyển tiếp record tới Container để ErrorHandler xử lý.

### 3.2. Thành phần 2: DefaultErrorHandler & FixedBackOff (Tầng Thử lại)
- Thay thế các ErrorHandler cũ bằng `DefaultErrorHandler` (chuẩn từ Spring Kafka 2.8+ / 3.x).
- Thiết lập chính sách `FixedBackOff(1000L, 3L)`:
  - Cho phép thử lại tối đa **3 lần** (`maxAttempts = 3`), mỗi lần cách nhau **1000ms**.
  - Tích hợp `RetryListener` để ghi nhận nhật ký cảnh báo chi tiết từng lần retry (Partition, Offset, số lần thử, nguyên nhân lỗi) phục vụ cho việc giám sát và cảnh báo.

### 3.3. Thành phần 3: DeadLetterPublishingRecoverer (Tầng Chuyển tiếp DLQ)
- Khi một record đã thử lại đủ 3 lần mà vẫn thất bại (hoặc gặp lỗi không thể khắc phục):
  - `DeadLetterPublishingRecoverer` được kích hoạt.
  - Tự động trích xuất toàn bộ payload gốc (kể cả raw bytes của chuỗi JSON hỏng) và publish sang topic Dead Letter Queue: `order-events.DLT`.
  - Đính kèm đầy đủ các metadata quan trọng vào Header của bản tin DLQ:
    + `kafka_dlt-original-topic`: Topic gốc sinh lỗi (`order-events`).
    + `kafka_dlt-original-partition`: Phân vùng xảy ra lỗi.
    + `kafka_dlt-original-offset`: Vị trí offset của tin nhắn hỏng.
    + `kafka_dlt-exception-message`: Chi tiết thông báo lỗi và stack trace.
- Cung cấp hai template chuyên biệt:
  + `KafkaTemplate<Object, Object>`: Sử dụng `JsonSerializer` cho các model nghiệp vụ.
  + `KafkaTemplate<Object, byte[]>`: Sử dụng `ByteArraySerializer` cho dữ liệu raw bytes của JSON hỏng.

### 3.4. Thành phần 4: Cơ chế Giải phóng Offset (Unblocking)
- Ngay sau khi `DeadLetterPublishingRecoverer` xác nhận đã xuất bản thành công bản tin lỗi sang topic DLQ:
  - Container tiến hành **commit offset** của bản tin đó trên topic chính `order-events`.
  - Vị trí con trỏ `Current Position` được tăng lên: `Current Offset = Offset hỏng + 1`.
  - Vấn đề Head-of-Line Blocking được giải quyết hoàn toàn: Consumer tiếp tục đọc và xử lý ngay các đơn hàng hợp lệ phía sau.

---

## 4. Bảng so sánh các chiến lược xử lý lỗi trong Kafka

| Tiêu chí | 1. Không xử lý (Mặc định ban đầu) | 2. Nuốt lỗi (Try-catch rỗng) | 3. Retry vô tận (Infinite Retry) | 4. Retry có giới hạn + DLQ (Giải pháp đề xuất) |
| :--- | :--- | :--- | :--- | :--- |
| **Khả năng giải phóng Consumer** | Bị kẹt hoàn toàn (Poison Pill) | Không bị kẹt | Bị kẹt vô tận | **Không bị kẹt, vận hành liên tục** |
| **Bảo toàn dữ liệu (Zero Data Loss)** | Không mất (nhưng treo hệ thống) | Mất dữ liệu vĩnh viễn (Data Loss) | Không mất (nhưng nghẽn toàn bộ) | **Bảo toàn 100% dữ liệu qua DLQ** |
| **Khả năng tự phục hồi lỗi tạm thời** | Không có | Không có | Có (nếu DB sống lại sau đó) | **Có (Thử lại 3 lần cho transient error)** |
| **Truy vết và khắc phục sự cố** | Khó khăn, ngập tràn log lỗi | Không thể truy vết do đã bị nuốt | Gây áp lực cực lớn lên DB/CPU | **Dễ dàng điều tra qua Header của DLQ** |
| **Ảnh hưởng tới đơn hàng hợp lệ** | Chặn toàn bộ đơn hàng phía sau | Không chặn đơn hàng khác | Chặn toàn bộ đơn hàng phía sau | **Đơn hàng hợp lệ xử lý ngay lập tức** |

---

## 5. Mối quan hệ giữa Kafka Partition, thứ tự tin nhắn và DLQ

### 5.1. Tính toàn vẹn thứ tự (Message Ordering) trong Partition
- Kafka chỉ đảm bảo tính toàn vẹn thứ tự tin nhắn **trong phạm vi từng Partition**, dựa vào Message Key (ví dụ: băm theo `productId` hoặc `orderId`).
- Khi một message bị lỗi và bị chuyển vào DLQ, thứ tự xử lý của riêng chuỗi sự kiện liên quan đến record đó có thể bị ảnh hưởng nếu các sự kiện tiếp theo cùng Key được xử lý trước khi DLQ được tái xử lý.

### 5.2. Giải pháp đảm bảo nghiệp vụ cho StoreX:
1. **Đối với lỗi sai định dạng JSON (Malformed JSON):**
   - Bản tin hoàn toàn vô nghĩa và không thể đọc được. Việc đẩy vào DLQ là bắt buộc để ngăn chặn chặn dòng chảy của các đơn hàng khác.
2. **Đối với lỗi nghiệp vụ phụ thuộc thứ tự nghiêm ngặt:**
   - Khi tái xử lý DLQ, quản trị viên sử dụng tool Replay hoặc Consumer chuyên trách để tái đẩy tin nhắn vào topic chính sau khi đã sửa lỗi dữ liệu.
   - Áp dụng cơ chế **Optimistic Locking** (Version check) hoặc **Idempotent Consumer** (kiểm tra trạng thái đơn hàng) để bảo đảm việc trừ kho không bị trùng lặp hoặc sai lệch số liệu.

---

## 6. Minh chứng thực nghiệm (Test Results & Verification)

Dự án đã được kiểm thử tự động toàn diện thông qua bộ test `InventoryConsumerIntegrationTest` kết hợp cùng **Embedded Kafka Cluster thực tế** (3 test cases) và `InventoryServiceTest` (5 test cases), đạt tỉ lệ thành công 100%.

### Kết quả chạy kiểm thử bằng Gradle:
```text
File                                                                    Tests Failures Errors
----                                                                    ----- -------- ------
TEST-com.storex.inventory.consumer.InventoryConsumerIntegrationTest.xml 3     0        0     
TEST-com.storex.inventory.service.InventoryServiceTest.xml              5     0        0     

BUILD SUCCESSFUL in 17s
8 tests completed, 0 failed, 0 skipped
```

### Chi tiết các kịch bản kiểm thử:
1. **Test Case 1 (`testValidMessage_ProcessedSuccessfully`):**
   - Gửi `OrderEvent` hợp lệ (`orderId: ORDER-101`, `quantity: 10`).
   - Consumer tiếp nhận ngay lập tức, tồn kho giảm từ 100 xuống 90.
   - Không có bất kỳ message nào bị chuyển vào DLQ.
2. **Test Case 2 (`testBusinessError_RetriesThreeTimesAndRoutesToDlq`):**
   - Gửi đơn hàng với mã sản phẩm lỗi `PROD-ERROR` để kích hoạt ngoại lệ RuntimeException.
   - Hệ thống thực hiện đúng: 1 lần xử lý ban đầu + 3 lần thử lại (tổng cộng 4 lần thực thi).
   - Sau khi retry 3 lần thất bại, message được tự động chuyển vào topic `order-events.DLT`.
   - `InventoryDlqConsumer` ghi nhận chính xác bản tin với key `ORDER-ERR-001`.
3. **Test Case 3 (`testMalformedJson_SentToDlqAndConsumerNotStuck`):**
   - Gửi trực tiếp chuỗi JSON bị thiếu ngoặc `}`: `{"orderId":"ORDER-MALFORMED","productId":"PROD-001","quantity":5`.
   - `ErrorHandlingDeserializer` và `DefaultErrorHandler` chặn lỗi ngay lập tức, chuyển chuỗi thô vào `order-events.DLT`.
   - Gửi tiếp ngay sau đó một `OrderEvent` hợp lệ (`orderId: ORDER-102`, `quantity: 15`).
   - Consumer xử lý thành công `ORDER-102`, tồn kho giảm từ 50 xuống 35.
   - Chứng minh Consumer **hoàn toàn không bị kẹt**, hiện tượng Head-of-Line Blocking được giải quyết triệt để.

---

## 7. Hướng dẫn chạy và kiểm thử dự án

### Yêu cầu môi trường:
- Java JDK 17 trở lên.
- Kết nối mạng để tải các dependency từ Maven Central trong lần chạy đầu tiên.

### Lệnh thực thi kiểm thử:
```bash
# Chay toan bo bo kiem thu voi Gradle
./gradlew test

# Xem chi tiet log thuc thi cua cac test case
./gradlew test --info
```

### Báo cáo kết quả kiểm thử:
Báo cáo chi tiết dạng HTML được Gradle tự động sinh tại:
`build/reports/tests/test/index.html`
