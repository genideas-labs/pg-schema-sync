#!/bin/bash

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# DB 설정
HOST="127.0.0.1"
PORT="54322"
USER="postgres"
PASSWORD="postgres"
DBNAME="postgres"

export PGPASSWORD=$PASSWORD

echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║         🚀 Clean Migration with Idle Transaction Cleanup      ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Kill all idle in transaction
echo -e "${BLUE}[Step 1/4] Killing idle in transaction connections...${NC}"
KILLED=$(psql -h $HOST -p $PORT -U $USER -d $DBNAME -t -c "
SELECT COUNT(*)
FROM pg_stat_activity 
WHERE datname = '$DBNAME' 
  AND state = 'idle in transaction'
  AND pid != pg_backend_pid();
")

if [ "$KILLED" -gt 0 ]; then
    echo -e "${YELLOW}  Found $KILLED idle in transaction connection(s)${NC}"
    psql -h $HOST -p $PORT -U $USER -d $DBNAME -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity 
    WHERE datname = '$DBNAME' 
      AND state = 'idle in transaction'
      AND pid != pg_backend_pid();
    " > /dev/null
    echo -e "${GREEN}  ✅ Killed $KILLED connection(s)${NC}"
else
    echo -e "${GREEN}  ✅ No idle in transaction connections found${NC}"
fi
echo ""

# Step 2: Wait a moment
echo -e "${BLUE}[Step 2/4] Waiting 3 seconds for DB to stabilize...${NC}"
sleep 3
echo ""

# Step 3: Show current connection status
echo -e "${BLUE}[Step 3/4] Current connection status:${NC}"
psql -h $HOST -p $PORT -U $USER -d $DBNAME -c "
SELECT 
    state,
    COUNT(*) as count
FROM pg_stat_activity 
WHERE datname = '$DBNAME'
GROUP BY state
ORDER BY count DESC;
"
echo ""

# Step 4: Run migration
echo -e "${BLUE}[Step 4/4] Starting migration...${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}⚠️  DO NOT interrupt the migration!${NC}"
echo ""

python -m src.pg_schema_sync --with-data

EXIT_CODE=$?

echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Migration completed successfully!${NC}"
else
    echo -e "${RED}❌ Migration failed with exit code: $EXIT_CODE${NC}"
    echo ""
    echo -e "${YELLOW}💡 Troubleshooting:${NC}"
    echo -e "   1. Check if there are still idle transactions:"
    echo -e "      ./migrate_clean.sh"
    echo -e "   2. Stop Docker services and try again:"
    echo -e "      docker stop supabase_realtime_oq-api supabase_rest_oq-api supabase_storage_oq-api"
    echo -e "   3. Check locks:"
    echo -e "      python check_connections.py"
fi

exit $EXIT_CODE

