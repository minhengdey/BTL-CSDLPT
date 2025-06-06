# phong_solution.py

import os
import csv
import time
from psycopg2 import sql
from psycopg2.extras import execute_batch
import phong_solution as Interface

# =============================== CẤU HÌNH CHUNG ===============================
BATCH_SIZE               = 100000
RANGE_TABLE_PREFIX       = 'range_part'
RROBIN_TABLE_PREFIX      = 'rrobin_part'
INPUT_FILE_PATH          = 'ratings.dat'

# **Điền password Postgres của bạn ở đây** (phải khớp với testHelper.getopenconnection)
DB_PASSWORD              = '123456'


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    Interface.loadratings(ratingstablename, ratingsfilepath, openconnection)


# =============================== 1. round robin partition ===============================



import time
import psycopg2
from multiprocessing import Pool

# Giả sử các biến toàn cục sau đã được định nghĩa:
#   RROBIN_TABLE_PREFIX: prefix cho tên bảng partition (ví dụ "rrobin_part_")
#   BATCH_SIZE: kích thước mỗi lần batch insert (ví dụ 1000)
#   COLUMNS = ('userid', 'movieid', 'rating')

def roundrobinpartition(ratingstablename, number_of_partitions, openconnection):
    start = time.time()
    conn = openconnection
    cur = conn.cursor()
    
    for i in range(number_of_partitions):
        # Tạo phân mảnh thứ i.
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)
        
        # Lấy các bản ghi từ bảng gốc ứng với phân mảnh thứ i
        cur.execute(f"""
            SELECT userid, movieid, rating FROM (
                SELECT 
                    userid, movieid, rating,
                    ROW_NUMBER() OVER (ORDER BY userid, movieid) - 1 AS row_number
                FROM {ratingstablename}
            ) AS sub
            WHERE MOD(row_number, {number_of_partitions}) = {i};
        """)
        
        # Thực hiện chèn dữ liệu vào phân mảnh thứ i theo từng batch 10000 bản ghi một.
        rows = cur.fetchall()
    
        batchinsert(RROBIN_TABLE_PREFIX + str(i), ('userid', 'movieid', 'rating'), rows, BATCH_SIZE, cur)
            
    
    end = time.time()
    print(f"[roundrobinpartition] Completed in {end - start:.2f} seconds.")
    cur.close()
    conn.commit()
    
    
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