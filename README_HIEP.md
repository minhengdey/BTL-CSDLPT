# RRobin Partition.
## V1
- Thực hiện truy vấn theo kiểu đơn giản.
- Các bước thực hiện.
  - Tạo bản ghi gốc.
  - Đối với mỗi phân mảnh.
    - Thực hiện truy vấn trên toàn bộ bảng gốc để lọc ra các hàng ứng với mảnh
    
```python
        cur.execute(f"""
            SELECT userid, movieid, rating FROM (
                SELECT 
                    userid, movieid, rating,
                    ROW_NUMBER() OVER (ORDER BY userid, movieid) - 1 AS row_number
                FROM {ratingstablename}
            ) AS sub
            WHERE MOD(row_number, {number_of_partitions}) = {i};
        """)

```
    - Từ các hàng đã được lọc mới tiếp tục chèn vào phân mảnh.
    
- Vấn đề gặp phải:
  - Do với mỗi phân mảnh đều phải truy vấn trên toàn bộ bảng gốc => tốn nhiều thời gian.
    - Thực tế nếu từ bảng gốc 10 triệu bản ghi mà chia thành:
      - 5 phân mảnh: tốn khoảng 31s => chấp nhận được.
      - 100 phân mảnh: tốn khoảng 215s => khá lâu.