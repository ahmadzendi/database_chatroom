import psycopg2
import os

PGHOST = os.environ.get("PGHOST", "shinkansen.proxy.rlwy.net")
PGPORT = os.environ.get("PGPORT", "38459")
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "isi password")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "railway")

conn = psycopg2.connect(
    dbname=POSTGRES_DB,
    user=PGUSER,
    password=PGPASSWORD,
    host=PGHOST,
    port=PGPORT
)
c = conn.cursor()
c.execute('''
CREATE TABLE IF NOT EXISTS request (
    id SERIAL PRIMARY KEY,
    data JSONB,
    updated_at TIMESTAMP DEFAULT NOW()
)
''')
conn.commit()
c.close()
conn.close()
print("Tabel request berhasil dibuat!")
