#!/bin/bash

# Setup Kafka Topics Script
# Creates necessary topics for the order processing system

BOOTSTRAP_SERVER="localhost:9092"

echo "=== Creating Kafka Topics ==="
echo ""

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 5

# Create orders topic (main topic)
echo "Creating 'orders' topic..."
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

# Create orders-dlq topic (Dead Letter Queue)
echo "Creating 'orders-dlq' topic..."
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --create \
  --topic orders-dlq \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --if-not-exists

# Create orders-aggregated topic (optional - for aggregated results)
echo "Creating 'orders-aggregated' topic..."
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --create \
  --topic orders-aggregated \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

echo ""
echo "=== Listing All Topics ==="
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list

echo ""
echo "=== Topic Details ==="
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --describe --topic orders
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --describe --topic orders-dlq
docker exec kafka kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --describe --topic orders-aggregated

echo ""
echo "✓ Topics created successfully!"
