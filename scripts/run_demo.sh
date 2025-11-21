#!/bin/bash

# Kafka Order Processing System - Demo Script
# Demonstrates the complete system with producer, consumer, and DLQ monitoring

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "  Kafka Order Processing System - Demo"
echo "=========================================="
echo ""

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Error: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    echo "✓ Docker is running"
}

# Function to check if services are ready
wait_for_services() {
    echo ""
    echo "⏳ Waiting for Kafka and Schema Registry to be ready..."
    
    MAX_RETRIES=30
    RETRY_COUNT=0
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
            echo "✓ Schema Registry is ready"
            break
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "   Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
        sleep 2
    done
    
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo "❌ Error: Services did not start in time"
        exit 1
    fi
    
    sleep 5  # Additional buffer
}

# Function to cleanup background processes
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    
    # Kill background processes
    if [ ! -z "$CONSUMER_PID" ]; then
        kill $CONSUMER_PID 2>/dev/null || true
    fi
    if [ ! -z "$DLQ_PID" ]; then
        kill $DLQ_PID 2>/dev/null || true
    fi
    
    echo "✓ Cleanup complete"
}

# Set up cleanup trap
trap cleanup EXIT INT TERM

# Step 1: Check Docker
echo "Step 1: Checking prerequisites..."
check_docker
echo ""

# Step 2: Start Docker Compose
echo "Step 2: Starting Kafka infrastructure..."
cd "$PROJECT_DIR"
docker compose up -d || docker-compose up -d

wait_for_services
echo ""

# Step 3: Create topics
echo "Step 3: Creating Kafka topics..."
chmod +x scripts/setup_topics.sh
./scripts/setup_topics.sh
echo ""

# Step 4: Start DLQ monitor in background
echo "Step 4: Starting DLQ monitor..."
cd "$PROJECT_DIR"
python3 src/consumer/dead_letter_queue_consumer.py > logs/dlq_consumer.log 2>&1 &
DLQ_PID=$!
echo "✓ DLQ monitor started (PID: $DLQ_PID)"
sleep 2
echo ""

# Step 5: Start consumer in background
echo "Step 5: Starting order consumer..."
python3 src/consumer/avro_order_consumer.py > logs/consumer.log 2>&1 &
CONSUMER_PID=$!
echo "✓ Consumer started (PID: $CONSUMER_PID)"
sleep 3
echo ""

# Step 6: Run producer
echo "Step 6: Producing orders..."
echo "=========================================="
python3 src/producer/kafka_order_producer.py
echo ""

# Step 7: Wait for processing
echo "Step 7: Waiting for message processing..."
sleep 5
echo ""

# Step 8: Show results
echo "Step 8: Results"
echo "=========================================="
echo ""

echo "📊 Consumer Log (last 30 lines):"
echo "----------------------------------------"
tail -n 30 logs/consumer.log
echo ""

echo "🚨 DLQ Monitor Log (last 20 lines):"
echo "----------------------------------------"
tail -n 20 logs/dlq_consumer.log || echo "No DLQ messages (this is good!)"
echo ""

# Step 9: Display Kafka UI info
echo "=========================================="
echo "✅ Demo Complete!"
echo "=========================================="
echo ""
echo "📌 Access Points:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - Schema Registry: http://localhost:8081"
echo "   - Consumer logs: logs/consumer.log"
echo "   - DLQ logs: logs/dlq_consumer.log"
echo ""
echo "🔧 Useful Commands:"
echo "   - View consumer logs: tail -f logs/consumer.log"
echo "   - View DLQ logs: tail -f logs/dlq_consumer.log"
echo "   - Stop services: docker-compose down"
echo "   - Restart demo: ./scripts/run_demo.sh"
echo ""
echo "Press Ctrl+C to stop background processes and exit..."

# Keep script running until interrupted
wait
