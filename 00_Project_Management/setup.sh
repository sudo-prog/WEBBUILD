#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_DIR="${PROJECT_DIR}/schema"
CONFIG_FILE="${PROJECT_DIR}/config/settings.json"

echo "============================================"
echo "  Supabase Australia — Quick Start"
echo "============================================"

# 1. Check Docker
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker not installed. Install Docker Desktop first."
    exit 1
fi

# 2. Start local Supabase via Docker Compose
echo ""
echo "[1/4] Starting local Supabase (Postgres on port 6543)..."
docker-compose up -d postgres

# Wait for healthy
echo "    Waiting for DB to become healthy..."
for i in $(seq 1 30); do
    if docker-compose exec -T postgres pg_isready -U postgres &>/dev/null; then
        echo "    ✅ Database ready"
        break
    fi
    echo -n "."
    sleep 2
    if [ $i -eq 30 ]; then
        echo ""
        echo "ERROR: Database failed to start"
        docker-compose logs postgres
        exit 1
    fi
done

# 3. Generate service role key (pw hash)
echo ""
echo "[2/4] Setting up credentials..."
# Generate a raw password for service role (simple approach)
SERVICE_PW="${SERVICE_PASSWORD:-supabase_service_$(date +%s)}"
echo "    Service role password: ${SERVICE_PW}"

# Create service role user
docker-compose exec -T postgres psql -U postgres -d postgres <<SQL 2>/dev/null || true
CREATE ROLE "supabase_service" WITH LOGIN PASSWORD '${SERVICE_PW}' CREATEDB;
SQL
echo "    Created supabase_service user"

# 4. Update settings.json with connection string
echo ""
echo "[3/4] Writing config file..."
mkdir -p "$(dirname "${CONFIG_FILE}")"

# Construct connection URL
CONN_URL="postgresql://supabase_service:${SERVICE_PW}@localhost:6543/postgres"

cat > "${CONFIG_FILE}" <<EOF
{
  "supabase": {
    "url": "http://localhost:6543",
    "anon_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.placeholder.local",
    "service_role_key": "${SERVICE_PW}"
  },
  "postgres": {
    "host": "127.0.0.1",
    "port": 6543,
    "database": "postgres",
    "user": "supabase_service",
    "password": "${SERVICE_PW}"
  },
  "ingestion": {
    "batch_size": 100,
    "retry_attempts": 3,
    "timeout_seconds": 30
  },
  "cities": {
    "sydney":   {"state": "NSW", "city": "Sydney"},
    "melbourne":{"state": "VIC", "city": "Melbourne"},
    "brisbane": {"state": "QLD", "city": "Brisbane"},
    "perth":    {"state": "WA",  "city": "Perth"},
    "adelaide": {"state": "SA",  "city": "Adelaide"},
    "hobart":   {"state": "TAS", "city": "Hobart"},
    "darwin":   {"state": "NT",  "city": "Darwin"},
    "canberra": {"state": "ACT",  "city": "Canberra"}
  }
}
EOF
echo "    Config written to ${CONFIG_FILE}"

# 5. Show env vars alternative
echo ""
echo "[4/4] Alternative: set these environment variables instead of config file:"
echo "  export SUPABASE_URL=\"http://localhost:6543\""
echo "  export SUPABASE_SERVICE_ROLE_KEY=\"${SERVICE_PW}\""

# 6. Next steps
echo ""
echo "============================================"
echo "  Next steps:"
echo "============================================"
echo ""
echo "1. Schema should auto-apply via Docker volume (schema/ mounted)."
echo "   If not, run manually:"
echo "   docker-compose exec -T postgres psql -U postgres -f /docker-entrypoint-initdb.d/001_initial_schema.sql"
echo ""
echo "2. Test connection:"
echo "   python3 -c \"import psycopg2; print('OK')\""
echo ""
echo "3. Run ingestion:"
echo "   python ingestion_pipeline.py --all"
echo ""
echo "4. Or single city:"
echo "   python ingestion_pipeline.py --city sydney"
echo ""
echo "To stop:  docker-compose down"
echo "To view logs: docker-compose logs -f postgres"
echo ""
