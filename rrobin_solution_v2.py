# phong_solution.py

import os
import csv
import time
from psycopg2 import sql
from psycopg2.extras import execute_batch
import phong_solution as Interface

# =============================== CẤU HÌNH CHUNG ===============================
BATCH_SIZE               = 10000000
RANGE_TABLE_PREFIX       = 'range_part'
RROBIN_TABLE_PREFIX      = 'rrobin_part'
INPUT_FILE_PATH          = 'ratings.dat'

# **Điền password Postgres của bạn ở đây** (phải khớp với testHelper.getopenconnection)
DB_PASSWORD              = '123456'




def loadratings(ratingstablename, ratingsfilepath, openconnection):
    Interface.loadratings(ratingstablename, ratingsfilepath, openconnection)
    



# =============================== 1. round robin partition ===============================
import time



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Phân chia theo Round-Robin: bản ghi i sẽ vào partition (i % numberofpartitions).
    Tạo numberofpartitions table với tên: RROBIN_TABLE_PREFIX + idx.
    Giả sử các table partition chưa tồn tại, nếu đã có, ta sẽ xóa sạch trước.
    """
    conn = openconnection
    cur = conn.cursor()
    insert_cur = conn.cursor()
    start = time.time()

    # 1. Xoá (nếu có) và tạo mới các table partition
    for i in range(numberofpartitions):
        tbl = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"""
            CREATE TABLE {tbl} (
                userid  INTEGER,
                movieid INTEGER,
                rating  REAL
            );
        """)

    # 2. Lấy tổng số bản ghi (tuỳ chọn, chỉ để in log)
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]

    # 3. Lấy lần lượt theo batch và chèn vào partition thích hợp
    #    Sử dụng i_row để tính index partition: part_index = i_row % numberofpartitions
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
    for i in range(numberofpartitions):
        if(tuple_inserts[i]):
            batchinsert(f"{RROBIN_TABLE_PREFIX}{i}",
                       ('userid', 'movieid', 'rating'),
                       tuple_inserts[i],
                       BATCH_SIZE, insert_cur)
    # 4. Commit và đóng cursor
    conn.commit()
    cur.close()
    insert_cur.close()

    end = time.time()
    print(f"[roundrobinpartition] Completed in {end - start:.2f} seconds. "
          f"Total rows processed: {total_rows}")

def batchinsert(tableName, columnTuples, dataTuples, batchSize, insertcur):
    for i in range(0, len(dataTuples), batchSize):
        batch = dataTuples[i:min(i + batchSize, len(dataTuples))]
        values_str = ", ".join(
            "(" + ", ".join(map(str, row)) + ")"
            for row in batch)
        insert_query = f"""INSERT INTO {tableName} ({', '.join(columnTuples)}) 
                           VALUES {values_str} """
        insertcur.execute(insert_query)

    

# =============================== 5. roundrobininsert ===============================
def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    
    # Lấy tổng số lượng phẩn mảnh round robin hiện có trong cơ sở dữ liệu.
    cur.execute("""
        SELECT COUNT(*) 
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND tablename LIKE %s;
    """, (RROBIN_TABLE_PREFIX + '%',))
    partition_number = cur.fetchone()[0]
    
    # Lấy tổng số lượng bản ghi hiện có từ bảng gốc.
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]

    # Xác định phân mảnh sẽ chứa bản ghi mới
    partition_index = total_rows % partition_number
    
    # Thực hiện chèn bản ghi vào phân mảnh tương ứng.
    cur.execute(f"""
        INSERT INTO {RROBIN_TABLE_PREFIX}{partition_index} (userid, movieid, rating)
        VALUES ({userid}, {movieid}, {rating});
    """)
    
    cur.close()
    conn.commit()