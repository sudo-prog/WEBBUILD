import os
import psycopg2
import socket

# Print environment
print("PG_PASSWORD:", repr(os.getenv("PG_PASSWORD")))
print("PGHOST:", repr(os.getenv("PGHOST")))
print("PGPORT:", repr(os.getenv("PGPORT")))
print("PGDATABASE:", repr(os.getenv("PGDATABASE")))
print("PGUSER:", repr(os.getenv("PGUSER")))

# Set up connection parameters
host = os.getenv("PGHOST", "localhost")
port = int(os.getenv("PGPORT", "6543"))
dbname = os.getenv("PGDATABASE", "postgres")
user = os.getenv("PGUSER", "supabase_service")
password = os.getenv("PGPASSWORD", "")

print(f"\nConnecting to {host}:{port} as {user} to database {dbname}")

# Test if port is open
sock = socket.socket()
try:
    sock.connect((host, int(port)))
    print("Port is open")
except Exception as e:
    print(f"Port connection failed: {e}")

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
    # Try to get more details
    import traceback
    traceback.print_exc()