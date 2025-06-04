# phong_solution.py

import os
import csv
import time
import psycopg2
from psycopg2 import sql
import multiprocessing as mp

# =============================== CẤU HÌNH CHUNG ===============================
BATCH_SIZE               = 10000
RANGE_TABLE_PREFIX       = 'range_part'
RROBIN_TABLE_PREFIX      = 'rrobin_part'
RROBIN_INSERT_SEQ        = 'rrobin_insert_seq'
INPUT_FILE_PATH          = 'ratings.dat'

# **Điền password Postgres của bạn ở đây** (phải khớp với testHelper.getopenconnection)
DB_PASSWORD              = 'minhanh2722004'


# =============================== HÀM TIỆN ÍCH CHUNG ===============================
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


# =============================== 1. loadratings ===============================
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


# =============================== 2. rangepartition (parallel) ===============================
def _range_worker(args):
    """
    Worker cho partition i: tạo/truncate bảng range_part{i}, 
    rồi INSERT ... SELECT WHERE rating ∈ [min,max].
    args = (i, ratingstablename, numberofpartitions, conn_info)
    """
    i, ratingstablename, numberofpartitions, conn_info = args
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()

    part_name = f"{RANGE_TABLE_PREFIX}{i}"
    # Tạo hoặc TRUNCATE
    cur.execute("""
        SELECT EXISTS (
          SELECT 1
            FROM pg_catalog.pg_tables
           WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
             AND tablename = %s
        );
    """, (part_name,))
    exists = cur.fetchone()[0]
    if exists:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(part_name)))
    else:
        cur.execute(sql.SQL("""
            CREATE TABLE {} (
                userid  INTEGER,
                movieid INTEGER,
                rating  REAL
            );
        """).format(sql.Identifier(part_name)))
    conn.commit()

    # Tính khoảng giá trị
    delta = 5.0 / numberofpartitions
    min_val = i * delta
    max_val = min_val + delta

    if i == 0:
        where_clause = "rating >= %s AND rating <= %s"
    else:
        where_clause = "rating > %s AND rating <= %s"

    query = sql.SQL("""
        INSERT INTO {} (userid, movieid, rating)
        SELECT userid, movieid, rating
          FROM {}
         WHERE {}
    """).format(
        sql.Identifier(part_name),
        sql.Identifier(ratingstablename),
        sql.SQL(where_clause)
    )
    cur.execute(query, (min_val, max_val))
    conn.commit()

    cur.close()
    conn.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tạo numberofpartitions bảng range_part0..range_part{n-1} và
    phân chia dữ liệu từ ratingstablename dựa trên rating ∈ [0,5].
    Thực hiện song song bằng multiprocessing.
    """
    # Lấy tham số kết nối (tham chiếu tới DB) từ openconnection
    dsn_params = openconnection.get_dsn_parameters()
    conn_info = {
        'dbname':   dsn_params['dbname'],
        'user':     dsn_params['user'],
        'password': DB_PASSWORD,
        'host':     dsn_params.get('host', 'localhost'),
        'port':     dsn_params.get('port', '5432')
    }
    # **Không đóng openconnection** ở đây, để testHelper có thể tiếp tục sử dụng.

    args_list = [
        (i, ratingstablename, numberofpartitions, conn_info)
        for i in range(numberofpartitions)
    ]

    start = time.time()
    num_workers = min(numberofpartitions, mp.cpu_count())
    with mp.Pool(processes=num_workers) as pool:
        pool.map(_range_worker, args_list)
    end = time.time()

    print(f"[rangepartition] Completed in {end - start:.2f} seconds.")


# =============================== 3. rangeinsert ===============================
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn một record mới vào đúng bảng range_partX dựa trên rating.
    """
    conn = openconnection
    cur = conn.cursor()

    num_parts = _count_partitions(RANGE_TABLE_PREFIX, conn)
    if num_parts == 0:
        cur.close()
        return

    delta = 5.0 / num_parts
    idx = int(rating / delta)
    if (rating % delta == 0) and (idx != 0):
        idx -= 1
    part_table = f"{RANGE_TABLE_PREFIX}{idx}"

    cur.execute(sql.SQL("""
        INSERT INTO {} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """).format(sql.Identifier(part_table)),
    (userid, itemid, rating))
    conn.commit()
    cur.close()


# =============================== 4. roundrobinpartition (parallel) ===============================
def _rrobin_worker(args):
    """
    Worker cho partition i: tạo/truncate bảng rrobin_part{i}, 
    rồi INSERT ... SELECT với modulo (ROW_NUMBER()).
    args = (i, ratingstablename, numberofpartitions, conn_info)
    """
    i, ratingstablename, numberofpartitions, conn_info = args
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()

    part_name = f"{RROBIN_TABLE_PREFIX}{i}"
    # Tạo hoặc TRUNCATE
    cur.execute("""
        SELECT EXISTS (
          SELECT 1
            FROM pg_catalog.pg_tables
           WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
             AND tablename = %s
        );
    """, (part_name,))
    exists = cur.fetchone()[0]
    if exists:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(part_name)))
    else:
        cur.execute(sql.SQL("""
            CREATE TABLE {} (
                userid  INTEGER,
                movieid INTEGER,
                rating  REAL
            );
        """).format(sql.Identifier(part_name)))
    conn.commit()

    query = sql.SQL("""
        INSERT INTO {} (userid, movieid, rating)
        SELECT userid, movieid, rating
        FROM (
            SELECT userid, movieid, rating,
                   (ROW_NUMBER() OVER (ORDER BY userid, movieid, rating) - 1) AS rn
            FROM {}
        ) AS sub
        WHERE (sub.rn %% %s) = %s;
    """).format(
        sql.Identifier(part_name),
        sql.Identifier(ratingstablename)
    )
    cur.execute(query, (numberofpartitions, i))
    conn.commit()

    cur.close()
    conn.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tạo numberofpartitions bảng rrobin_part0..rrobin_part{n-1} và
    phân phối dữ liệu từ ratingstablename bằng round‐robin.
    Thực hiện song song bằng multiprocessing.
    """
    # Tạo (hoặc recreate) sequence rrobin_insert_seq
    conn_main = openconnection
    cur_main = conn_main.cursor()
    cur_main.execute("""
        SELECT EXISTS (
          SELECT 1
            FROM pg_catalog.pg_sequences
           WHERE sequencename = %s
        );
    """, (RROBIN_INSERT_SEQ,))
    seq_exists = cur_main.fetchone()[0]
    if seq_exists:
        cur_main.execute(sql.SQL("DROP SEQUENCE {}").format(sql.Identifier(RROBIN_INSERT_SEQ)))
        conn_main.commit()

    cur_main.execute(sql.SQL("""
        CREATE SEQUENCE {} 
        START 0 
        MINVALUE 0 
        MAXVALUE %s 
        INCREMENT 1 
        CYCLE;
    """).format(sql.Identifier(RROBIN_INSERT_SEQ)),
    (numberofpartitions - 1,))
    conn_main.commit()
    cur_main.close()

    # Lấy tham số kết nối
    dsn_params = openconnection.get_dsn_parameters()
    conn_info = {
        'dbname':   dsn_params['dbname'],
        'user':     dsn_params['user'],
        'password': DB_PASSWORD,
        'host':     dsn_params.get('host', 'localhost'),
        'port':     dsn_params.get('port', '5432')
    }
    # **Không đóng openconnection** để testHelper tiếp tục sử dụng.

    args_list = [
        (i, ratingstablename, numberofpartitions, conn_info)
        for i in range(numberofpartitions)
    ]

    start = time.time()
    num_workers = min(numberofpartitions, mp.cpu_count())
    with mp.Pool(processes=num_workers) as pool:
        pool.map(_rrobin_worker, args_list)
    end = time.time()

    print(f"[roundrobinpartition] Completed in {end - start:.2f} seconds.")


# =============================== 5. roundrobininsert ===============================
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn mới một record vào bảng rrobin_partX dựa trên sequence rrobin_insert_seq.
    """
    conn = openconnection
    cur = conn.cursor()

    cur.execute(sql.SQL("SELECT nextval(%s)"), (RROBIN_INSERT_SEQ,))
    idx = cur.fetchone()[0]
    part_table = f"{RROBIN_TABLE_PREFIX}{idx}"

    cur.execute(sql.SQL("""
        INSERT INTO {} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """).format(sql.Identifier(part_table)),
    (userid, itemid, rating))
    conn.commit()
    cur.close()
