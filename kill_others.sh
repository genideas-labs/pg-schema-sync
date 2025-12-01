#!/bin/bash

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# DB 설정
HOST="127.0.0.1"
PORT="54322"
USER="postgres"
DBNAME="postgres"
PASSWORD="postgres"

export PGPASSWORD=$PASSWORD

echo -e "${BLUE}🔪 Killing non-migration connections...${NC}\n"

# 1. idle in transaction 연결 제거
echo -e "${YELLOW}[1] Killing 'idle in transaction' connections...${NC}"
IDLE_TX=$(psql -h $HOST -p $PORT -U $USER -d $DBNAME -t -c "
SELECT COUNT(*)
FROM pg_stat_activity 
WHERE datname = '$DBNAME' 
  AND state = 'idle in transaction'
  AND pid != pg_backend_pid();
")

if [ "$IDLE_TX" -gt 0 ]; then
    psql -h $HOST -p $PORT -U $USER -d $DBNAME -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity 
    WHERE datname = '$DBNAME' 
      AND state = 'idle in transaction'
      AND pid != pg_backend_pid();
    " > /dev/null
    echo -e "  ${GREEN}✅ Killed $IDLE_TX idle in transaction${NC}"
else
    echo -e "  ${GREEN}✅ No idle in transaction found${NC}"
fi

# 2. PostgREST 연결 제거
echo -e "\n${YELLOW}[2] Killing PostgREST connections...${NC}"
POSTGREST=$(psql -h $HOST -p $PORT -U $USER -d $DBNAME -t -c "
SELECT COUNT(*)
FROM pg_stat_activity 
WHERE datname = '$DBNAME' 
  AND application_name LIKE '%PostgREST%'
  AND pid != pg_backend_pid();
")

if [ "$POSTGREST" -gt 0 ]; then
    psql -h $HOST -p $PORT -U $USER -d $DBNAME -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity 
    WHERE datname = '$DBNAME' 
      AND application_name LIKE '%PostgREST%'
      AND pid != pg_backend_pid();
    " > /dev/null
    echo -e "  ${GREEN}✅ Killed $POSTGREST PostgREST connection(s)${NC}"
else
    echo -e "  ${GREEN}✅ No PostgREST connections found${NC}"
fi

# 3. 오래된 idle 연결 제거 (5분 이상)
echo -e "\n${YELLOW}[3] Killing old idle connections (> 5 min)...${NC}"
OLD_IDLE=$(psql -h $HOST -p $PORT -U $USER -d $DBNAME -t -c "
SELECT COUNT(*)
FROM pg_stat_activity 
WHERE datname = '$DBNAME' 
  AND state = 'idle'
  AND state_change < NOW() - INTERVAL '5 minutes'
  AND pid != pg_backend_pid();
")

if [ "$OLD_IDLE" -gt 0 ]; then
    psql -h $HOST -p $PORT -U $USER -d $DBNAME -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity 
    WHERE datname = '$DBNAME' 
      AND state = 'idle'
      AND state_change < NOW() - INTERVAL '5 minutes'
      AND pid != pg_backend_pid();
    " > /dev/null
    echo -e "  ${GREEN}✅ Killed $OLD_IDLE old idle connection(s)${NC}"
else
    echo -e "  ${GREEN}✅ No old idle connections found${NC}"
fi

# 4. 현재 남은 연결 표시
echo -e "\n${BLUE}📊 Remaining connections:${NC}"
psql -h $HOST -p $PORT -U $USER -d $DBNAME -c "
SELECT 
    state,
    COUNT(*) as count,
    STRING_AGG(DISTINCT application_name, ', ') as apps
FROM pg_stat_activity 
WHERE datname = '$DBNAME'
  AND pid != pg_backend_pid()
GROUP BY state
ORDER BY count DESC;
"

echo -e "\n${GREEN}✅ Cleanup completed!${NC}"

