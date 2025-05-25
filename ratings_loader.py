import csv
import psycopg2
from psycopg2 import sql, extras
from configparser import ConfigParser
import argparse

BATCH_SIZE = 10000

def load_db_config(path='config.ini'):
    cfg = ConfigParser()
    cfg.read(path)
    pg = cfg['postgres']
    return dict(host=pg['host'], port=pg.getint('port'),
                dbname=pg['dbname'], user=pg['user'],
                password=pg['password'])

def LoadRatings(input_path: str, config_path: str = 'config.ini') -> None:
    # 1) Kết nối DB
    cfg = load_db_config(config_path)
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()

    # 2) Tạo schema và bảng Ratings trong schema movielens
    cur.execute("CREATE SCHEMA IF NOT EXISTS movielens;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movielens.ratings (
        userid  INT,
        movieid INT,
        rating  FLOAT,
        PRIMARY KEY (userid, movieid)
    );
    """)
    conn.commit()

    # 3) Chuẩn bị batch-insert với ON CONFLICT để bỏ qua duplicate
    insert_q = sql.SQL(
        "INSERT INTO movielens.ratings (userid, movieid, rating) VALUES %s ON CONFLICT DO NOTHING"
    )

    # 4) Đọc file và insert theo batch
    with open(input_path, 'r', encoding='utf-8') as f:
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
                conn.commit()
                batch.clear()

        if batch:
            extras.execute_values(cur, insert_q, batch)
            conn.commit()

    # 5) Đóng kết nối
    cur.close()
    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load MovieLens ratings into PostgreSQL')
    parser.add_argument('input_path', help='Đường dẫn tới file ratings.dat')
    parser.add_argument('--config', default='config.ini', help='Đường dẫn tới file cấu hình DB')
    args = parser.parse_args()

    print(f"Starting load from {args.input_path} using config {args.config}")
    LoadRatings(args.input_path, args.config)
    print("Finished loading ratings.")