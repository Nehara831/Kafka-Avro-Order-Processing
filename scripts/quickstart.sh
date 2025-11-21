#!/bin/bash

# Quick Start Script for Kafka Order Processing System

echo "========================================"
echo "  Kafka Order Processing System"
echo "  Quick Start Guide"
echo "========================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running!"
    echo "   Please start Docker Desktop and run this script again."
    exit 1
fi

echo "✓ Docker is running"
echo ""

# Install Python dependencies
echo "📦 Installing Python dependencies..."
# Try conda/pip depending on what's available
if command -v conda &> /dev/null && [ -n "$CONDA_DEFAULT_ENV" ]; then
    echo "   Using conda environment: $CONDA_DEFAULT_ENV"
    pip install -q -r requirements.txt
elif command -v pip3 &> /dev/null; then
    pip3 install -q --user -r requirements.txt 2>/dev/null || pip3 install -q -r requirements.txt
else
    pip install -q --user -r requirements.txt 2>/dev/null || pip install -q -r requirements.txt
fi

if [ $? -eq 0 ]; then
    echo "✓ Dependencies installed"
else
    echo "⚠️  Some dependencies may need manual installation"
    echo "   If you see errors later, run: pip install -r requirements.txt"
fi
echo ""

# Start Docker services
echo "🚀 Starting Kafka infrastructure..."
docker compose up -d || docker-compose up -d
echo ""

# Wait for services
echo "⏳ Waiting for services to be ready (this may take 30-60 seconds)..."
sleep 30

# Check Schema Registry
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
        echo "✓ Schema Registry is ready"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ Services did not start in time"
    echo "   Check logs with: docker compose logs"
    exit 1
fi
echo ""

# Create topics
echo "📋 Creating Kafka topics..."
chmod +x scripts/setup_topics.sh
./scripts/setup_topics.sh
echo ""

# Create logs directory
mkdir -p logs

echo "========================================"
echo "✅ Setup Complete!"
echo "========================================"
echo ""
echo "🎯 Next Steps:"
echo ""
echo "1. Run the complete demo:"
echo "   ./scripts/run_demo.sh"
echo ""
echo "2. OR run components manually:"
echo "   Terminal 1: python3 src/consumer/order_consumer.py"
echo "   Terminal 2: python3 src/consumer/dlq_consumer.py"
echo "   Terminal 3: python3 src/producer/order_producer.py"
echo ""
echo "3. Access Kafka UI:"
echo "   http://localhost:8080"
echo ""
echo "4. View Schema Registry:"
echo "   http://localhost:8081/subjects"
echo ""
echo "📚 For more information, see README.md"
echo ""
