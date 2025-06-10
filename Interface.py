import os
import csv
import time
import psycopg2
from psycopg2 import sql, extras
import multiprocessing as mp
from multiprocessing import Pool

# Cấu hình chung
BATCH_SIZE               = 10000
RANGE_TABLE_PREFIX       = 'range_part'
RROBIN_TABLE_PREFIX      = 'rrobin_part'
RROBIN_INSERT_SEQ        = 'rrobin_insert_seq'
INPUT_FILE_PATH          = 'test_data.dat'

# Password Postgre DB
DB_PASSWORD              = '123456'


# Hàm chèn dữ liệu theo từng batch vào mảnh phân vùng
def batchinsert(tableName, columnTuples, dataTuples, batchSize, insertcur):
    for i in range(0, len(dataTuples), batchSize):
        batch = dataTuples[i:min(i + batchSize, len(dataTuples))]
        values_str = ", ".join(
            "(" + ", ".join(map(str, row)) + ")"
            for row in batch)
        insert_query = f"""INSERT INTO {tableName} ({', '.join(columnTuples)}) 
                           VALUES {values_str} """
        insertcur.execute(insert_query)

# Hàm chuyển đổi dữ liệu từ file .dat sang file .csv
def _preprocess_raw_to_csv(raw_path, csv_path):
    with open(raw_path, 'r', encoding='utf-8') as fin, \
         open(csv_path, 'w', encoding='utf-8', newline='') as fout:
        writer = csv.writer(fout)
        for line in fin:
            parts = line.strip().split("::")
            if len(parts) < 3:
                continue
            writer.writerow([parts[0], parts[1], parts[2]])

# Đếm số lượng mảnh sau khi chia 
def _count_partitions(prefix, openconnection):
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

# Hàm COPY dữ liệu vào DB
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    # Mở kết nối
    conn = openconnection
    cur = conn.cursor()
    # Tên file CSV tạm 
    temp_csv = ratingstablename + '_temp.csv'

    # Thời gian bắt đầu
    start = time.time()

    # Xóa bảng nếu đã tồn tại
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(ratingstablename)))
    conn.commit()

    # Tạo bảng ratings gốc mới
    cur.execute(sql.SQL("""
        CREATE TABLE {} (
            userid  INTEGER NOT NULL,
            movieid INTEGER NOT NULL,
            rating  REAL    NOT NULL
        );
    """).format(sql.Identifier(ratingstablename)))
    conn.commit()

    # Chuyển đổi dữ liệu từ file .dat sang file .csv
    _preprocess_raw_to_csv(ratingsfilepath, temp_csv)

    # Thực hiện COPY từ file CSV vào bảng ratings
    with open(temp_csv, 'r', encoding='utf-8') as f:
        cur.copy_expert(
            sql=f"COPY {ratingstablename}(userid, movieid, rating) FROM STDIN WITH (FORMAT csv)",
            file=f
        )
    conn.commit()

    # Xóa file CSV tạm
    try:
        os.remove(temp_csv)
    except OSError:
        pass

    # In ra thời gian chạy và đóng con trỏ DB
    end = time.time()
    print(f"[loadratings] Completed in {end - start:.2f} seconds.")

    cur.close()


# Hàm worker cho thực hiện rangepartition song song và ghi dữ liệu vào các mảnh
def _range_worker(args):
    # Lấy thông số kết nối DB
    i, ratingstablename, numberofpartitions, conn_info = args
    conn = psycopg2.connect(**conn_info)
    part_name = f"{RANGE_TABLE_PREFIX}{i}"

    # Tính toán khoảng giá trị cho phân vùng
    delta = 5.0 / numberofpartitions
    min_val = 0.0
    for _ in range(i):
        min_val += delta
    max_val = min_val + delta
    if i == numberofpartitions - 1:
        max_val = 5.0

    # Điều kiện INSERT vào mảnh
    if i == 0:
        where_clause = "rating >= %s AND rating <= %s"
    else:
        where_clause = "rating > %s AND rating <= %s"

    # Tạo con trỏ riêng cho đọc dữ liệu từ bảng ratings
    read_cur = conn.cursor()
    
    # Thực hiện truy vấn để lấy dữ liệu từ bảng ratings
    read_cur.execute(
        sql.SQL("SELECT userid, movieid, rating FROM {} WHERE " + where_clause)
        .format(sql.Identifier(ratingstablename)),
        (min_val, max_val)
    )

    # Khởi tạo con trỏ ghi dữ liệu vào mảnh
    write_cur = conn.cursor()

    # Thực hiện INSERT theo BATCH_SIZE
    while True:
        batch = read_cur.fetchmany(BATCH_SIZE)
        if not batch:
            break

        batchinsert(part_name, ['userid', 'movieid', 'rating'], batch, BATCH_SIZE, write_cur)

    # Đóng con trỏ đọc và ghi, commit và đóng kết nối
    conn.commit()
    read_cur.close()
    write_cur.close()
    conn.close()

# Hàm phân vùng theo khoảng giá trị (rangepartition)
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    # Đặt thời gian bắt đầu
    start = time.time()
    # Thực hiện đánh index cho cột rating bảng ratings
    with openconnection.cursor() as cur:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_rating ON {ratingstablename}(rating);")
        openconnection.commit()

    # Khởi tạo con trỏ để tạo các mảnh
    setup_cur = openconnection.cursor()

    # Hàm tạo các mảnh phân vùng
    for i in range(numberofpartitions) :
        part_name = f"{RANGE_TABLE_PREFIX}{i}"
        setup_cur.execute(f"DROP TABLE IF EXISTS {part_name};")
        setup_cur.execute(f"""
                CREATE TABLE {part_name} (
                    userid  INTEGER,
                    movieid INTEGER,
                    rating  REAL
                );
            """)
        openconnection.commit()
    setup_cur.close()

    # Lấy thông tin kết nối từ đối tượng openconnection
    dsn_params = openconnection.get_dsn_parameters()
    conn_info = {
        'dbname':   dsn_params['dbname'],
        'user':     dsn_params['user'],
        'password': DB_PASSWORD,
        'host':     dsn_params.get('host', 'localhost'),
        'port':     dsn_params.get('port', '5432')
    }

    # Tạo tham số cho hàm _range_worker
    args_list = [
        (i, ratingstablename, numberofpartitions, conn_info)
        for i in range(numberofpartitions)
    ]

    # Xác định số lượng worker và thực hiện phân vùng song song
    num_workers = mp.cpu_count()
    with mp.Pool(processes=num_workers) as pool:
        pool.map(_range_worker, args_list)
    
    # Thời gian kết thúc
    end = time.time()
    print(f"[rangepartition] Completed in {end - start:.2f} seconds.")

# Hàm chèn bản ghi mới vào mảnh phân vùng theo khoảng giá trị
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    # Lấy thời gian bắt đầu
    start = time.time()
    
    # Tạo kết nối và con trỏ
    conn = openconnection
    cur = conn.cursor()

    # Đếm số lượng mảnh phân vùng hiện tại
    num_parts = _count_partitions(RANGE_TABLE_PREFIX, conn)
    if num_parts == 0:
        cur.close()
        return

    # Kiểm tra xem rating nằm trong mảnh nào
    delta = 5.0 / num_parts
    idx = 0
    min_val = 0.0
    for i in range(num_parts):
        if i == 0:
            if min_val <= rating <= min_val + delta :
                idx = i
                break
        else :
            if min_val < rating <= min_val + delta :
                idx = i
                break
        min_val = min_val + delta
    part_table = f"{RANGE_TABLE_PREFIX}{idx}"

    # Thực hiện INSERT vào mảnh tương ứng
    cur.execute(sql.SQL("""
        INSERT INTO {} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """).format(sql.Identifier(part_table)),
    (userid, itemid, rating))
    
    # In ra thời gian thực hiện chèn dữ liệu
    end = time.time()
    print(f"[rangeinsert] Inserted into {part_table} in {end - start:.2f} seconds.")
    
    # Commit các thay đổi và đóng con trỏ
    conn.commit()
    cur.close()


# Hàm worker để chèn dữ liệu song song vào các mảnh phân vùng theo round-robin
def _batchinsert_worker(args):
    # Lấy các tham số từ args
    # tableName = tên mảnh cần chèn dữ liệu
    # columnTuples = tuple chứa tên các cột
    # dataTuples = dữ liệu cần chèn
    # batchSize = kích thước batch
    # conn_params = thông tin kết nối DB
    tableName, columnTuples, dataTuples, batchSize, conn_params = args

    # Mở kết nối và con trỏ
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Thực hiện chèn dữ liệu theo từng batch trong mảnh này
    for i in range(0, len(dataTuples), batchSize):
        batch = dataTuples[i : i + batchSize]
        batchinsert(tableName, columnTuples, batch, batchSize, cur)

    # Commit các thay đổi và đóng kết nối
    # Đóng con trỏ và kết nối
    conn.commit()
    cur.close()
    conn.close()

# Hàm phân vùng theo round-robin (roundrobinpartition)
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    
    # Mở kết nối và con trỏ
    conn = openconnection
    cur = conn.cursor()
    
    # Lấy thời gian bắt đầu
    start = time.time()

    # Thực hiện tạo các mảnh phân vùng theo round-robin
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

    # Lấy dữ liệu từ bảng ratings 
    cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename};")
    row_index = 0
    batch = cur.fetchmany(BATCH_SIZE)

    # Tạo danh sách để chứa dữ liệu cần chèn vào từng mảnh
    tuple_inserts = [[] for _ in range(numberofpartitions)]
    
    # Thực hiện phân phối dữ liệu vào các mảnh theo round-robin theo từng batch
    while batch:
        for row in batch:
            # Xác định mảnh cần chèn dữ liệu
            part_index = row_index % numberofpartitions
            
            # Lưu dữ liệu tạm vào mảng danh sách dữ liệu các mảnh
            tuple_inserts[part_index].append((row[0], row[1], row[2]))
            row_index += 1
        batch = cur.fetchmany(BATCH_SIZE)
        
    # Đóng con trỏ đọc dữ liệu và commit các thay đổi
    cur.close()
    conn.commit()

    # Lấy các thông số kết nối để sử dụng trong multiprocessing
    conn_params = conn.get_dsn_parameters()
    conn_params['password'] = DB_PASSWORD  
    
    # Tạo danh sách các task để thực hiện chèn dữ liệu song song
    # Mỗi task sẽ ứng với một mảnh 
    tasks = []
    for i in range(numberofpartitions):
        dataTuples = tuple_inserts[i]
        if not dataTuples:
            continue

        tableName = f"{RROBIN_TABLE_PREFIX}{i}"
        columnTuples = ('userid', 'movieid', 'rating')
        tasks.append((tableName, columnTuples, dataTuples, BATCH_SIZE, conn_params))

    # Tạo pool các task cho thực hiện song song
    # Thực hiện chèn dữ liệu song song vào các mảnh 
    pool = Pool(processes=len(tasks))
    pool.map(_batchinsert_worker, tasks)
    pool.close()
    pool.join()
    
    # THời gian kết thúc 
    end = time.time()
    print(f"[roundrobinpartition] Completed in {end - start:.2f} seconds. ")
    


# Hàm chèn bản ghi mới vào các mảnh phân vùng theo round-robin
def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    # Lấy thời gian bắt đầu
    start = time.time()
    
    # Mở kết nối và con trỏ
    conn = openconnection
    cur = conn.cursor()
    
    # Đếm số lượng mảnh phân vùng hiện tại
    cur.execute("""
        SELECT COUNT(*) 
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND tablename LIKE %s;
    """, (RROBIN_TABLE_PREFIX + '%',))
    partition_number = cur.fetchone()[0]
    
    
    # Đếm số lượng bản ghi trong bảng ratings
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]

    # Tính toán chỉ số mảnh phân vùng cần chèn dữ liệu
    partition_index = (total_rows) % partition_number
    
    # Thực hiện chèn dữ liệu vào mảnh phân vùng theo round-robin
    cur.execute(f"""
        INSERT INTO {RROBIN_TABLE_PREFIX}{partition_index} (userid, movieid, rating)
        VALUES ({userid}, {movieid}, {rating});
    """)
    
    # In ra thời gian thực hiện chèn dữ liệu
    end = time.time()
    print(f"[roundrobininsert] Inserted into {RROBIN_TABLE_PREFIX}{partition_index} in {end - start:.2f} seconds.")
    
    # Commit các thay đổi và đóng con trỏ
    cur.close()
    conn.commit()