# phong_solution.py

import os
import csv
import time
from psycopg2 import sql
from psycopg2.extras import execute_batch

# =============================== CẤU HÌNH CHUNG ===============================
BATCH_SIZE               = 100000
RANGE_TABLE_PREFIX       = 'range_part'
RROBIN_TABLE_PREFIX      = 'rrobin_part'
INPUT_FILE_PATH          = 'ratings.dat'

# **Điền password Postgres của bạn ở đây** (phải khớp với testHelper.getopenconnection)
DB_PASSWORD              = '123456'

def _preprocess_raw_to_csv(raw_path, csv_path):
    """
    Đọc file raw (delimiter='::'), ghi ra CSV (delimiter=',') chỉ giữ 3 cột đầu:
    userid, movieid, rating.
    """
    with open(raw_path, 'r', encoding='utf-8') as fin, \
         open(csv_path, 'w', encoding='utf-8', newline='') as fout:
        writer = csv.writer(fout)
        for line in fin:
            parts = line.strip().split("::")
            if len(parts) < 3:
                continue
            writer.writerow([parts[0], parts[1], parts[2]])


def _count_partitions(prefix, openconnection):
    """
    Đếm số bảng có tên giống prefix + '%'.
    """
    cur = openconnection.cursor()
    cur.execute("""
        SELECT COUNT(*)
          FROM pg_catalog.pg_tables
         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
           AND tablename LIKE %s;
    """, (prefix + '%',))
    cnt = cur.fetchone()[0]
    cur.close()
    return cnt


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Tạo table ratingstablename (userid INT, movieid INT, rating REAL),
    sau đó bulk‐load (~10M rows) với COPY. Dữ liệu gốc ở ratingsfilepath
    có delimiter '::'; ta sẽ chuyển thành CSV tạm rồi COPY.
    """
    conn = openconnection
    cur = conn.cursor()
    temp_csv = ratingstablename + '_temp.csv'

    start = time.time()

    # 1) Nếu đã có table thì DROP (để testHelper tạo lại clean)
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(ratingstablename)))
    conn.commit()

    # 2) Tạo table mới
    cur.execute(sql.SQL("""
        CREATE TABLE {} (
            userid  INTEGER NOT NULL,
            movieid INTEGER NOT NULL,
            rating  REAL    NOT NULL
        );
    """).format(sql.Identifier(ratingstablename)))
    conn.commit()

    # 3) Preprocess raw '::' → CSV
    _preprocess_raw_to_csv(ratingsfilepath, temp_csv)

    # 4) COPY từ CSV vào table
    with open(temp_csv, 'r', encoding='utf-8') as f:
        cur.copy_expert(
            sql=f"COPY {ratingstablename}(userid, movieid, rating) FROM STDIN WITH (FORMAT csv)",
            file=f
        )
    conn.commit()

    # 5) Xóa file tạm
    try:
        os.remove(temp_csv)
    except OSError:
        pass

    end = time.time()
    print(f"[loadratings] Completed in {end - start:.2f} seconds.")

    cur.close()


# =============================== 1. round robin partition ===============================



import time
import psycopg2
from multiprocessing import Pool

# Giả sử các biến toàn cục sau đã được định nghĩa:
#   RROBIN_TABLE_PREFIX: prefix cho tên bảng partition (ví dụ "rrobin_part_")
#   BATCH_SIZE: kích thước mỗi lần batch insert (ví dụ 1000)
#   COLUMNS = ('userid', 'movieid', 'rating')

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


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Phân chia theo Round-Robin và chèn song song cho mỗi partition.
    openconnection: psycopg2 connection đã mở
    conn_params: dict chứa { 'dbname', 'user', 'password', 'host', 'port' }
    """
    conn = openconnection
    cur = conn.cursor()
    start = time.time()

    # 1. Tạo (hoặc xóa rồi tạo) các bảng partition
    for i in range(numberofpartitions):
        tbl = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"DROP TABLE IF EXISTS {tbl};")
        cur.execute(f"""
            CREATE TABLE {tbl} (
                userid  INTEGER,
                movieid INTEGER,
                rating  REAL
            );
        """)

    # 2. Đọc toàn bộ dữ liệu từ bảng gốc theo batch
    cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename};")
    row_index = 0
    batch = cur.fetchmany(BATCH_SIZE)

    # Khởi tạo list để gom dữ liệu cho từng partition
    tuple_inserts = [[] for _ in range(numberofpartitions)]
    
    # Duyệt qua từng batch và phân phối dữ liệu vào các partition
    while batch:
        for row in batch:
            # Phân phối từng row vào từng partition theo round-robin
            part_index = row_index % numberofpartitions
            tuple_inserts[part_index].append((row[0], row[1], row[2]))
            row_index += 1
        batch = cur.fetchmany(BATCH_SIZE)
    cur.close()
    conn.commit()


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