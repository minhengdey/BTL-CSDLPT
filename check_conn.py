import psycopg2
from configparser import ConfigParser

def load_db_config(path='config.ini'):
    cfg = ConfigParser()
    cfg.read(path)
    pg = cfg['postgres']
    return {
        'host': pg['host'],
        'port': pg.getint('port'),
        'dbname': pg['dbname'],
        'user': pg['user'],
        'password': pg['password'],
    }

if __name__ == '__main__':
    conn = psycopg2.connect(**load_db_config())
    print("✅ Kết nối thành công với PostgreSQL!")
    conn.close()