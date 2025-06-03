import csv
import psycopg2
from psycopg2 import sql, extras
from configparser import ConfigParser
import argparse

BATCH_SIZE = 1000

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'


# =============================== PHẦN TỰ CODE ===============================

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()

    delta = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'

    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)

        # Tạo bảng con
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)

        # Tạo điều kiện lọc
        if i == 0:
            condition = f"rating >= {minRange} AND rating <= {maxRange}"
        else:
            condition = f"rating > {minRange} AND rating <= {maxRange}"

        # Truy vấn toàn bộ dữ liệu phù hợp
        cur.execute(f"""
            SELECT userid, movieid, rating 
            FROM {ratingstablename}
            WHERE {condition};
        """)

        rows = cur.fetchall()

        # Nếu không có dòng nào thì bỏ qua
        if not rows:
            continue

        sql_insert_prefix = f"""
            INSERT INTO {table_name} (userid, movieid, rating) VALUES
        """

        count = 0
        values_batch = ""

        for row in rows:
            values_batch += f"({row[0]}, {row[1]}, {row[2]}),"
            count += 1

            if count == BATCH_SIZE:
                sql = sql_insert_prefix + values_batch.rstrip(',') + ";"
                cur.execute(sql)
                count = 0
                values_batch = ""

        # Chèn phần dư còn lại
        if count > 0:
            sql = sql_insert_prefix + values_batch.rstrip(',') + ";"
            cur.execute(sql)

    cur.close()
    conn.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    cur.execute("""
        SELECT COUNT(*) 
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND tablename LIKE %s;
    """, (RANGE_TABLE_PREFIX + '%',))
    partition_number = cur.fetchone()[0]

    delta = 5 / partition_number
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

import psycopg2

DATABASE_NAME = 'dds_assgn1'

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname=dbname)
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    

def getopenconnection(user='postgres', password='minhanh2722004', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """

    print(f"Starting load from {ratingsfilepath}")

    create_db(DATABASE_NAME)

    # 1) Kết nối DB
    con = openconnection
    cur = con.cursor()

    # 2) Tạo bảng Ratings
    cur.execute("create table " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    con.commit()

    # 3) Chuẩn bị batch-insert với ON CONFLICT để bỏ qua duplicate
    insert_q = sql.SQL(
        "INSERT INTO " +  ratingstablename + "(userid, movieid, rating) VALUES %s ON CONFLICT DO NOTHING"
    )

    # 4) Đọc file và insert theo batch
    with open(ratingsfilepath, 'r', encoding='utf-8') as f:
        reader = csv.reader((line.replace('::', ',') for line in f), delimiter=',')
        batch = []
        for idx, row in enumerate(reader, 1):
            try:
                user, movie, rating, *_ = row
                batch.append((int(user), int(movie), float(rating)))
            except Exception as e:
                print(f"[Line {idx}] parse error: {e}")
                continue

            if len(batch) >= BATCH_SIZE:
                extras.execute_values(cur, insert_q, batch)
                con.commit()
                batch.clear()

        if batch:
            extras.execute_values(cur, insert_q, batch)
            con.commit()
    
    cur.execute("alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
    con.commit()

    # 5) Đóng kết nối
    cur.close()
    con.commit()

    print("Finished loading ratings.")


# def rangepartition(ratingstablename, numberofpartitions, openconnection):
#     """
#     Function to create partitions of main table based on range of ratings.
#     """
#     con = openconnection
#     cur = con.cursor()
#     delta = 5 / numberofpartitions
#     RANGE_TABLE_PREFIX = 'range_part'
#     for i in range(0, numberofpartitions):
#         minRange = i * delta
#         maxRange = minRange + delta
#         table_name = RANGE_TABLE_PREFIX + str(i)
#         cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
#         if i == 0:
#             cur.execute(
#                 "insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(
#                     minRange) + " and rating <= " + str(maxRange) + ";")
#         else:
#             cur.execute(
#                 "insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(
#                     minRange) + " and rating <= " + str(maxRange) + ";")
#     cur.close()
#     con.commit()


def roundrobinpartition(ratingstablename, numberofpatitrions, openconnection):
    conn = openconnection
    cur = conn.cursor()

    for i in range(numberofpatitrions):
        # Tạo phân mảnh thứ i.
        cur.execute(f"""
            CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME} INTEGER,
                {MOVIE_ID_COLNAME} INTEGER,
                {RATING_COLNAME} FLOAT
            );
        """)

        # Lấy các bản ghi từ bảng gốc ứng với phân mảnh thứ i
        cur.execute(f"""
            SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME} FROM (
                SELECT 
                    {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME},
                    ROW_NUMBER() OVER () - 1 AS row_number
                FROM {ratingstablename}
            ) AS sub
            WHERE MOD(row_number, {numberofpatitrions}) = {i};
        """)

        # Thực hiện chèn dữ liệu vào phân mảnh thứ i theo từng batch 10000 bản ghi một.
        rows = cur.fetchall()

        # Câu lệnh insert gốc
        sql_insert = f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{i} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES """

        count = 0
        for row in rows:
            sql_insert += f" ({row[0]}, {row[1]}, {row[2]})"
            count += 1
            # Nếu đã đủ 10000 bản ghi thì thực hiện chèn vào cơ sở dữ liệu.
            if count == 10000:
                cur.execute(sql_insert + ";")
                count = 0
                sql_insert = f"""
                    INSERT INTO {RROBIN_TABLE_PREFIX}{i} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}) VALUES"""
            else:
                sql_insert += ","

        if count > 0:
            sql_insert = sql_insert.rstrip(",")
            cur.execute(sql_insert + ";")

    cur.close()
    conn.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
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
    total_rows = 0

    # Xác định phân mảnh sẽ chứa bản ghi mới
    partition_index = total_rows % partition_number

    # Thực hiện chèn bản ghi vào phân mảnh tương ứng.
    cur.execute(f"""
        INSERT INTO {RROBIN_TABLE_PREFIX}{partition_index} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
        VALUES (%s, %s, %s);
    """, (userid, itemid, rating))

    cur.close()
    conn.commit()


# def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
#     """
#     Function to insert a new row into the main table and specific partition based on range rating.
#     """
#     con = openconnection
#     cur = con.cursor()
#     RANGE_TABLE_PREFIX = 'range_part'
#     numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
#     delta = 5 / numberofpartitions
#     index = int(rating / delta)
#     if rating % delta == 0 and index != 0:
#         index = index - 1
#     table_name = RANGE_TABLE_PREFIX + str(index)
#     cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(
#         itemid) + "," + str(rating) + ");")
#     cur.close()
#     con.commit()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count