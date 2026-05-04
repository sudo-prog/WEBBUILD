#!/usr/bin/env bash
# Alternative start script using plain Docker (no docker-compose plugin required)

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_DIR="${PROJECT_DIR}/schema"
CONTAINER_NAME="supabase_postgres"
IMAGE="supabase/postgres:16.1.0"

echo "Starting Supabase PostgreSQL using plain Docker..."

# Pull image
echo "[1/3] Pulling image ${IMAGE}..."
docker pull "${IMAGE}"

# Stop existing container
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Stopping existing container..."
    docker stop "${CONTAINER_NAME}" 2>/dev/null || true
    docker rm "${CONTAINER_NAME}" 2>/dev/null || true
fi

# Generate password
SERVICE_PW="${SERVICE_PASSWORD:-supabase_service_$(date +%s)}"
echo "    Service password: ${SERVICE_PW}"

# Create network if not exists
NETWORK_NAME="supabase_net"
if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK_NAME}$"; then
    echo "[2/3] Creating network ${NETWORK_NAME}..."
    docker network create "${NETWORK_NAME}"
fi

# Run container
echo "[3/3] Starting container on port 6543..."
docker run -d \
    --name "${CONTAINER_NAME}" \
    --network "${NETWORK_NAME}" \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD="${SERVICE_PW}" \
    -e POSTGRES_DB=postgres \
    -v "${SCHEMA_DIR}:/docker-entrypoint-initdb.d:ro" \
    "${IMAGE}"

# Wait for ready
echo "Waiting for database..."
for i in $(seq 1 30); do
    if docker exec "${CONTAINER_NAME}" pg_isready -U postgres &>/dev/null; then
        echo "✅ Database ready on localhost:6543"
        break
    fi
    echo -n "."
    sleep 2
    if [ $i -eq 30 ]; then
        echo ""
        echo "ERROR: Database did not start"
        docker logs "${CONTAINER_NAME}"
        exit 1
    fi
done

# Create service role user
echo "Creating service role user..."
docker exec "${CONTAINER_NAME}" psql -U postgres -d postgres <<SQL 2>/dev/null || true
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_service') THEN
      CREATE ROLE "supabase_service" WITH LOGIN PASSWORD '${SERVICE_PW}' CREATEDB;
   END IF;
END
\$\$;
SQL
echo "✅ Service role created"

# Write config
CONFIG_FILE="${PROJECT_DIR}/config/settings.json"
mkdir -p "$(dirname "${CONFIG_FILE}")"
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
    "batch_size": 100
  },
  "cities": {
    "sydney": {"state": "NSW", "city": "Sydney"},
    "melbourne": {"state": "VIC", "city": "Melbourne"},
    "brisbane": {"state": "QLD", "city": "Brisbane"},
    "perth": {"state": "WA", "city": "Perth"},
    "adelaide": {"state": "SA", "city": "Adelaide"},
    "hobart": {"state": "TAS", "city": "Hobart"},
    "darwin": {"state": "NT", "city": "Darwin"},
    "canberra": {"state": "ACT", "city": "Canberra"}
  }
}
EOF
echo "✅ Config written to ${CONFIG_FILE}"

echo ""
echo "============================================"
echo "  Supabase local instance ready!"
echo "============================================"
echo ""
echo "Connection string:"
echo "  postgresql://supabase_service:${SERVICE_PW}@localhost:6543/postgres"
echo ""
echo "Next:"
echo "  1. Test: python3 -c \"import psycopg2; print('OK')\""
echo "  2. Schema: should auto-run (check logs if needed)"
echo "  3. Ingest: python ingestion_pipeline.py --all"
echo ""
echo "To stop: docker stop supabase_postgres && docker rm supabase_postgres"
echo "To view logs: docker logs supabase_postgres"
echo ""
