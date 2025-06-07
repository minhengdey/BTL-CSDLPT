import psycopg2
from configparser import ConfigParser

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='phonggda123', dbname='postgres',
                      host='localhost', port=5432):
    print(dbname)
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

if __name__ == '__main__':
    conn = None
    try:
        conn = getopenconnection(dbname=DATABASE_NAME)
        print("✅ Kết nối thành công với PostgreSQL!")
    except Exception as e:
        print(f"❌ Kết nối thất bại: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass
