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


    - Từ các hàng đã được lọc sẽ được chèn thêm vào phân mảnh

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


## V2 Optimzie query

- Tại phiên bản trước:
  - Thực hiện select và insert trên cùng 1 bảng.
  - Câu lệnh select thực hiện query trên toàn bộ bảng và lọc theo điều kiện để lấy được các hàng ứng với phân mảnh đó.
  - Số lần select trên toàn bộ bảng sẽ phụ thuộc vào số phân mảnh
  - => tốn nhiều thời gian khi số lượng phân mảnh tăng cao.
- Cải tiến.
  - Chỉ thực hiện truy vấn select trên toàn bộ bảng 1 lần.
  - Thực hiện lưu tạm các bảng thỏa vào mảng `tuple_inserts` trong đó mỗi phần tử chứa danh sách các bản ghi sẽ được chèn vào phân mảnh tương ứng.
  - Ở đây cũng chỉ thực hiện fetch theo BATCH_SIZE phòng trường hợp bộ nhớ không đủ.
``` python
    cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename};")
    row_index = 0
    batch = cur.fetchmany(BATCH_SIZE)
    
    
    tuple_inserts = [[] for _ in range(numberofpartitions)]

    while batch:
        # Tập hợp các hàng cho từng partition trong batch này
        # Tạo dict mapping part_index -> list of rows

        for row in batch:
            part_index = row_index % numberofpartitions
            tuple_inserts[part_index].append((row[0], row[1], row[2]))
            row_index += 1
 

        # Đọc batch tiếp
        batch = cur.fetchmany(BATCH_SIZE)
```

- Tại bước chèn cũng thực hiện tương tự bản trước.

```python

    for i in range(numberofpartitions):
        if(tuple_inserts[i]):
            batchinsert(f"{RROBIN_TABLE_PREFIX}{i}",
                       ('userid', 'movieid', 'rating'),
                       tuple_inserts[i],
                       BATCH_SIZE, insert_cur)

```

- Kết quả:
  - Với bảng gốc 10 triệu bản ghi, chia thành 100 phần mảnh, thời gian còn khoảng 27s.
  - Với bảng gốc 10 triệu bản ghi, chia thành 5 phân mảnh, thời gian cũng tương tự, khoảng 27s.

