# RRobin Partition.
## V1 Naive Approach
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

- Bổ sung:
  - Ngoài ra đối với bảng gốc 10 000 000 bản ghi và chia thành 5 phân mảnh.
  - Mỗi phân mảnh sẽ phải chèn khoảng 2 000 000 bản ghi.
  - Nếu thực hiện chèn trong 1 truy vấn sql có thể gây ra vấn đề về bộ nhớ gồm 2 triệu bản ghi gốc và over head do thực hiện chèn bằng python
  - => Bổ sung thêm tùy chọn insert theo batch, có thể tùy chỉnh kích thước batch theo máy mạnh hay yếu bằng cách thay đổi biến BATCH_SIZE.
  - Hàm batchinsert sẽ được triển khai nhưu dưới đây

```python
def batchinsert(tableName, columnTuples, dataTuples, batchSize, insertcur):
    for i in range(0, len(dataTuples), batchSize):
        batch = dataTuples[i:min(i + batchSize, len(dataTuples))]
        values_str = ", ".join(
            "(" + ", ".join(map(str, row)) + ")"
            for row in batch)
        insert_query = f"""INSERT INTO {tableName} ({', '.join(columnTuples)}) 
                           VALUES {values_str} """
        insertcur.execute(insert_query)

```
- Vấn đề gặp phải:

  - Do với mỗi phân mảnh đều phải truy vấn trên toàn bộ bảng gốc => tốn nhiều thời gian.
    - Thực tế nếu từ bảng gốc 10 triệu bản ghi mà chia thành:
      - 5 phân mảnh: tốn khoảng 31s => chấp nhận được.
      - 100 phân mảnh: tốn khoảng 215s => khá lâu.
    - => Phải tối ưu query.


## V2 