# Round Robin Partition.
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
    - Thực hiện lặp lại như vậy cho đến hết các phân mảnh.

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

## V3 Optimize Insert

- Do các bản ghi thực hiện insert vào các phân mảnh độc lập với nhau
  - => Có thể thực hiện song song hóa việc chèn mà không bị conflict với nhau
- Triển khai
  - Triển khai hàm _batchinsert_worker để mở một connect riêng đến phân mảnh tương ứng và thực hiện chèn.
```python
def _batchinsert_worker(args):
    """
    Worker cho mỗi partition:
    args = (tableName, columnTuples, dataTuples, batchSize, conn_params)
    """
    tableName, columnTuples, dataTuples, batchSize, conn_params = args

    # Mỗi tiến trình con tự mở connection riềng bằng conn_params
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    for i in range(0, len(dataTuples), batchSize):
        batch = dataTuples[i : i + batchSize]
        batchinsert(tableName, columnTuples, batch, batchSize, cur)

    conn.commit()
    cur.close()
    conn.close()
```

  - Triển khai trong hàm rrobinpartition chính:
    - Thực hiện tạo một list các task ứng với mỗi phân mảnh.
    - Tạo pool để thực hiện song song cho các task vừa tạo.

```python
    # 3. Tạo tasks cho mỗi partition (truyền conn_params (thông số của connection))
    conn_params = conn.get_dsn_parameters()
    conn_params['password'] = DB_PASSWORD  
    tasks = []
    for i in range(numberofpartitions):
        dataTuples = tuple_inserts[i]
        if not dataTuples:
            continue

        tableName = f"{RROBIN_TABLE_PREFIX}{i}"
        columnTuples = ('userid', 'movieid', 'rating')
        tasks.append((tableName, columnTuples, dataTuples, BATCH_SIZE, conn_params))

    # 4. Chạy song song với Pool
    pool = Pool(processes=len(tasks))
    pool.map(_batchinsert_worker, tasks)
    pool.close()
    pool.join()
    
    end = time.time()

    print(f"[roundrobinpartition] Completed in {end - start:.2f} seconds. ")
```

- Kết quả: tự làm nốt nhé, làm lại cả mấy cái trên nữa.





