# phong_solution.py

import os
import csv
import time
import psycopg2
from psycopg2 import sql

# =============================== CẤU HÌNH CHUNG ===============================
BATCH_SIZE               = 10000
RANGE_TABLE_PREFIX       = 'range_part'
RROBIN_TABLE_PREFIX      = 'rrobin_part'
RROBIN_INSERT_SEQ        = 'rrobin_insert_seq'
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
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    insert_cur = conn.cursor()
    start = time.time()
    
    # Khởi tạo các partition
    for i in range(numberofpartitions):
        cur.execute(("""
                            
            CREATE TABLE IF NOT EXISTS {} (
                userid  INTEGER,
                movieid INTEGER,
                rating  REAL
            );
        """).format((f"{RROBIN_TABLE_PREFIX}{i}")))

    # Lấy tổng số lượng bản ghi trong bảng gốc
    cur.execute((f"SELECT COUNT(*) FROM {ratingstablename}"))
    total_rows = cur.fetchone()[0]
    
    # Khởi tạo các biến số để phân chia bản ghi vào các partition
    
    # Số lượng bản ghi tối thiểu mỗi phần
    min_rows_per_part = total_rows // numberofpartitions
    
    # Biến để theo dõi số lượng bản ghi đã chèn vào mỗi phần
    part_row_count = 0
    
    # Biến để theo dõi chỉ số phần hiện tại
    part_index = 0
    
    # Lấy tất cả các bản ghi từ bảng gốc
    cur.execute((f"SELECT userid, movieid, rating FROM {ratingstablename}"))
    while True:
        # Nếu đã đạt đến số lượng bản ghi tối thiểu cho phần hiện tại, 
        # chuyển sang phần tiếp theo
        if(part_row_count == min_rows_per_part):
            part_row_count = 0
            part_index += 1
            # Nếu đã đạt đến số lượng phân mảnh, thoát khỏi vòng lặp
            if part_index >= numberofpartitions:
                break
            continue
 
        # Lấy số lượng bản ghi tối đa có thể chèn vào phần hiện tại
        # (tối đa là BATCH_SIZE hoặc số lượng bản ghi còn thiếu trong phần)
        fetch_size = min(BATCH_SIZE, min_rows_per_part - part_row_count)
        part_row_count += fetch_size
        
        rows = cur.fetchmany(fetch_size)
        # Tạo câu lệnh SQL để insert nhiều bản ghi
        if not rows:
            break
        sql_insert = (f"""
            INSERT INTO {RROBIN_TABLE_PREFIX+str(part_index)} 
            (userid, movieid, rating) 
            VALUES
        """)
        for row in rows:
            sql_insert += (f"""
                ({row[0]}, {row[1]}, {row[2]}),
            """)
        # Loại bỏ dấu phẩy cuối cùng
        sql_insert = sql_insert.rstrip().rstrip(',')
        sql_insert += ";"
        
        # Thực hiện chèn bản ghi vào phân mảnh tương ứng
        insert_cur.execute(sql_insert)
    
    
    # Thực hiện chèn các bản ghi còn lại nếu có
    part_index = 0
    while True:
        row = cur.fetchone()
        if not row:
            break
        insert_cur.execute(f"""                   
            INSERT INTO {RROBIN_TABLE_PREFIX+str(part_index)}
            (userid, movieid, rating)
            VALUES ({row[0]}, {row[1]}, {row[2]});
        """)
        part_index += 1
        
        
        
    # Kết thúc transaction
    conn.commit()
    cur.close()
    insert_cur.close()

    # Lấy số đo thời gian
    end = time.time()
    print(f"[roundrobinpartition] Completed in {end - start:.2f} seconds.")


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