# Thư mục Load Links

Thư mục này chứa các file `.txt` với danh sách links JSON cần extract.

## Cách sử dụng:

1. Tạo file `.txt` (ví dụ: `links.txt`) trong thư mục này
2. Mỗi dòng chứa một URL link đến file JSON
3. Dòng bắt đầu bằng `#` sẽ được coi là comment và bị bỏ qua
4. Ví dụ nội dung file:

```
# Danh sách links dữ liệu thời tiết
https://example.com/api/thunderstorms.json
https://example.com/api/rainfall.json
https://example.com/api/temperature.json
```

## Lưu ý:

- Nếu có nhiều file `.txt`, code sẽ tự động load file đầu tiên tìm được
- Có thể chỉ định đường dẫn file cụ thể khi gọi hàm `load_links_from_file(path)`

