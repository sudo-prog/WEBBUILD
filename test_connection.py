import os
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "6543")),
    dbname=os.getenv("PGDATABASE", "postgres"),
    user=os.getenv("PGUSER", "supabase_service"),
    password=os.getenv("PGPASSWORD", "")
)
print("✅ Connected successfully!")
conn.close()