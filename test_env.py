import os
import sys

print("PG_PASSWORD:", repr(os.getenv("PG_PASSWORD")))
print("PG_HOST:", repr(os.getenv("PG_HOST")))
print("PG_PORT:", repr(os.getenv("PG_PORT")))
print("PG_DATABASE:", repr(os.getenv("PG_DATABASE")))
print("PG_USER:", repr(os.getenv("PG_USER")))

import psycopg2
conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "6543")),
    dbname=os.getenv("PG_DATABASE", "postgres"),
    user=os.getenv("PG_USER", "supabase_service"),
    password=os.getenv("PG_PASSWORD", "")
)
print("✅ Connected successfully!")
conn.close()