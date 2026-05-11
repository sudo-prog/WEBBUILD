import os
import psycopg2

# Print environment
print("PG_PASSWORD:", repr(os.getenv("PG_PASSWORD")))
print("PG_HOST:", repr(os.getenv("PG_HOST")))
print("PG_PORT:", repr(os.getenv("PG_PORT")))
print("PG_DATABASE:", repr(os.getenv("PG_DATABASE")))
print("PG_USER:", repr(os.getenv("PG_USER")))

# Set up connection parameters using the same names as the pipeline script
host = os.getenv("PG_HOST", "localhost")
port = int(os.getenv("PG_PORT", "6543"))
dbname = os.getenv("PG_DATABASE", "postgres")
user = os.getenv("PG_USER", "supabase_service")
password = os.getenv("PG_PASSWORD", "")

print(f"\nConnecting to {host}:{port} as {user} to database {dbname}")
print(f"Password: {password}")

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    print("✅ Connected successfully!")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
    import traceback
    traceback.print_exc()