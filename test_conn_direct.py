import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=6543,
    dbname="postgres",
    user="supabase_service",
    password="supabase_service_1777905407"
)
print("✅ Connected successfully!")
conn.close()