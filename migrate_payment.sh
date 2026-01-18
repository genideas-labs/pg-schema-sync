#!/bin/bash
# Payment DB 마이그레이션 스크립트

echo "======================================"
echo "💳 PAYMENT DB Migration Starting..."
echo "======================================"
echo ""
echo "Source: SUPABASE PAYMENT DB"
echo "Target: GCP PROD PAYMENT DB"
echo ""

# venv 활성화
if [ -d "venv" ]; then
    echo "🐍 Activating virtual environment..."
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo "🐍 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️  Warning: No virtual environment found (venv or .venv)"
fi
echo ""

# 1단계: 스키마 마이그레이션
echo "📋 Step 1: Schema Migration"
echo "======================================"
python -m src.pg_schema_sync --config config_payment.yaml --commit

if [ $? -ne 0 ]; then
    echo "❌ Schema migration failed. Aborting."
    exit 1
fi

echo ""
echo "✅ Schema migration completed!"
echo ""

# 2단계: 데이터 마이그레이션
echo "📦 Step 2: Data Migration"
echo "======================================"
python -m src.pg_schema_sync --config config_payment.yaml --commit --with-data

if [ $? -ne 0 ]; then
    echo "❌ Data migration failed."
    exit 1
fi

echo ""
echo "======================================"
echo "✅ PAYMENT DB Migration Completed"
echo "======================================"

