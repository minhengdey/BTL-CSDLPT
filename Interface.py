#!/usr/bin/env python3
"""
Assignment 1: Data partitioning for MovieLens ratings
Updated to use schema `movielens_demo` and `partitions`, Python3
Includes CLI via argparse.
"""
import psycopg2
from psycopg2 import extensions, sql, extras
import csv
import argparse

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='minhanh2722004', dbname='postgres', host='localhost', port=5432):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


def create_db(dbname, conn_params=None):
    params = conn_params or {}
    con = getopenconnection(dbname='postgres', **params)
    con.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
    if not cur.fetchone():
        cur.execute(sql.SQL(f"CREATE DATABASE {dbname};"))
    cur.close()
    con.close()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS movielens_demo;")
    cur.execute(sql.SQL(
        "CREATE TABLE IF NOT EXISTS movielens_demo.{} (userid INT, movieid INT, rating FLOAT, PRIMARY KEY(userid, movieid));"
    ).format(sql.Identifier(ratingstablename)))
    conn.commit()
    insert_q = sql.SQL(
        "INSERT INTO movielens_demo.{} (userid, movieid, rating) VALUES %s ON CONFLICT DO NOTHING"
    ).format(sql.Identifier(ratingstablename))
    with open(ratingsfilepath, 'r', encoding='utf-8') as f:
        reader = csv.reader((line.replace('::', ',') for line in f), delimiter=',')
        batch = []
        for idx, row in enumerate(reader, start=1):
            try:
                u, m, r, *_ = row
                batch.append((int(u), int(m), float(r)))
            except Exception as e:
                print(f"[loadratings] parse error line {idx}: {e}")
                continue
            if len(batch) >= 10000:
                extras.execute_values(cur, insert_q, batch)
                conn.commit()
                batch.clear()
        if batch:
            extras.execute_values(cur, insert_q, batch)
            conn.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS partitions;")
    interval = 5.0 / numberofpartitions
    for i in range(numberofpartitions):
        part_name = f"range_part{i}"
        cur.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS partitions.{} (LIKE movielens_demo.{});"
        ).format(sql.Identifier(part_name), sql.Identifier(ratingstablename)))
        low = 0 if i == 0 else i * interval
        high = (i+1) * interval
        op_low = ">=" if i == 0 else ">"
        cur.execute(sql.SQL(
            "INSERT INTO partitions.{p} SELECT * FROM movielens_demo.{t} WHERE rating {op_low} %s AND rating <= %s"
        ).format(p=sql.Identifier(part_name), t=sql.Identifier(ratingstablename), op_low=sql.SQL(op_low)), (low, high))
    conn.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS partitions;")
    # initialize rr_meta
    cur.execute("DROP TABLE IF EXISTS partitions.rr_meta;")
    cur.execute("CREATE TABLE partitions.rr_meta (idx INT);")
    cur.execute("INSERT INTO partitions.rr_meta VALUES (0);")
    for i in range(numberofpartitions):
        part = f"rr_part{i}"
        cur.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS partitions.{} (LIKE movielens_demo.{});"
        ).format(sql.Identifier(part), sql.Identifier(ratingstablename)))
    conn.commit()
    cur.execute(sql.SQL(
        "SELECT userid, movieid, rating FROM movielens_demo.{} ORDER BY userid;"
    ).format(sql.Identifier(ratingstablename)))
    rows = cur.fetchall()
    for idx, (u, m, r) in enumerate(rows):
        part = f"rr_part{idx % numberofpartitions}"
        cur.execute(sql.SQL(
            "INSERT INTO partitions.{} (userid, movieid, rating) VALUES (%s, %s, %s);"
        ).format(sql.Identifier(part)), (u, m, r))
    conn.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS partitions;")
    # ensure rr_meta exists
    cur.execute("INSERT INTO partitions.rr_meta SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM partitions.rr_meta);")
    cur.execute(sql.SQL(
        "INSERT INTO movielens_demo.{} (userid, movieid, rating) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"
    ).format(sql.Identifier(ratingstablename)), (userid, itemid, rating))
    cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname = 'partitions' AND tablename LIKE 'rr_part%';")
    N = cur.fetchone()[0]
    cur.execute("SELECT idx FROM partitions.rr_meta;")
    next_idx = cur.fetchone()[0]
    part = f"rr_part{next_idx}"
    cur.execute(sql.SQL(
        "INSERT INTO partitions.{} (userid, movieid, rating) VALUES (%s, %s, %s);"
    ).format(sql.Identifier(part)), (userid, itemid, rating))
    cur.execute("UPDATE partitions.rr_meta SET idx = (idx + 1) % %s;", (N,))
    conn.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS partitions;")
    cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname = 'partitions' AND tablename LIKE 'range_part%';")
    N = cur.fetchone()[0]
    interval = 5.0 / N
    idx = min(int(rating / interval), N - 1)
    part = f"range_part{idx}"
    cur.execute(sql.SQL(
        "INSERT INTO partitions.{} (userid, movieid, rating) VALUES (%s, %s, %s);"
    ).format(sql.Identifier(part)), (userid, itemid, rating))
    conn.commit()
    cur.close()


def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname = 'partitions' AND tablename LIKE %s;", (prefix + '%',))
    cnt = cur.fetchone()[0]
    cur.close()
    return cnt


def main():
    parser = argparse.ArgumentParser(description='MovieLens Partitioning CLI')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p1 = sub.add_parser('loadratings')
    p1.add_argument('table')
    p1.add_argument('file')

    p2 = sub.add_parser('rangepart')
    p2.add_argument('table')
    p2.add_argument('n', type=int)

    p3 = sub.add_parser('rrpart')
    p3.add_argument('table')
    p3.add_argument('n', type=int)

    p4 = sub.add_parser('rrinsert')
    p4.add_argument('table')
    p4.add_argument('userid', type=int)
    p4.add_argument('itemid', type=int)
    p4.add_argument('rating', type=float)

    p5 = sub.add_parser('rinsert')
    p5.add_argument('table')
    p5.add_argument('userid', type=int)
    p5.add_argument('itemid', type=int)
    p5.add_argument('rating', type=float)

    args = parser.parse_args()
    conn = getopenconnection()

    if args.cmd == 'loadratings':
        loadratings(args.table, args.file, conn)
    elif args.cmd == 'rangepart':
        rangepartition(args.table, args.n, conn)
    elif args.cmd == 'rrpart':
        roundrobinpartition(args.table, args.n, conn)
    elif args.cmd == 'rrinsert':
        roundrobininsert(args.table, args.userid, args.itemid, args.rating, conn)
    elif args.cmd == 'rinsert':
        rangeinsert(args.table, args.userid, args.itemid, args.rating, conn)

    conn.close()


if __name__ == '__main__':
    main()