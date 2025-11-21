# Kafka Avro Order Processing

This project demonstrates a Kafka-based order processing system using Avro serialization. It includes producer, consumer, aggregator, and retry logic, with Docker support for easy setup.

## Features
- Kafka producer for order events
- Avro schema for order data (`schemas/order.avsc`)
- Consumer for processing orders and handling dead-letter queue
- Aggregator for price data
- Retry handler for failed messages
- Utility functions for Kafka operations
- Shell scripts for quickstart, setup, running demo, and stopping services
- Docker Compose for environment setup

## Project Structure
```
docker-compose.yml
requirements.txt
logs/
schemas/
  order.avsc
scripts/
  quickstart.sh
  run_demo.sh
  setup_topics.sh
  stop.sh
src/
  aggregator/
    price_aggregator.py
  consumer/
    avro_order_consumer.py
    dead_letter_queue_consumer.py
  producer/
    kafka_order_producer.py
  retry/
    retry_handler.py
  utils/
    kafka_utils.py
```

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Kafka

### Setup & Run
1. **Start services with Docker Compose:**
   ```sh
   docker-compose up -d
   ```
2. **Install Python dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
3. **Run quickstart script:**
   ```sh
   bash scripts/quickstart.sh
   ```
4. **Run demo:**
   ```sh
   bash scripts/run_demo.sh
   ```
5. **Stop services:**
   ```sh
   bash scripts/stop.sh
   ```

## Scripts
- `quickstart.sh`: Sets up Kafka topics and environment
- `run_demo.sh`: Runs the demo order processing
- `setup_topics.sh`: Creates required Kafka topics
- `stop.sh`: Stops all running services

## Avro Schema
The order schema is defined in `schemas/order.avsc`.

## Contributing
Feel free to fork and submit pull requests.

## License
MIT
