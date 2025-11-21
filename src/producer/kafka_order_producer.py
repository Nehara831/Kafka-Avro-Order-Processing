#!/usr/bin/env python3
"""
Order Producer
Produces order messages to Kafka with JSON serialization
(Note: For Avro, see order_producer_avro.py - using JSON for simplicity)
"""

import time
import random
import json
import logging
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaOrderProducer:
    """Kafka producer for order messages using JSON serialization"""
    
    def __init__(self, kafka_broker_address='localhost:9092'):
        """
        Initialize the order producer
        
        Args:
            kafka_broker_address: Kafka broker address
        """
        self.kafka_broker_address = kafka_broker_address
        
        # Producer configuration
        self.producer_config = {
            'bootstrap.servers': kafka_broker_address,
            
            # Reliability settings
            'acks': 'all',  # Wait for all replicas
            'retries': 3,  # Built-in retries
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,  # Exactly-once semantics
            
            # Performance settings
            'batch.size': 16384,
            'linger.ms': 10,  # Wait 10ms for batching
            'compression.type': 'snappy',
            
            # Timeout settings
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 120000,
        }
        
        # Initialize producer
        self.kafka_producer = Producer(self.producer_config)
        
        logger.info(f"Order Producer initialized - Broker: {kafka_broker_address}")
    
    def delivery_callback(self, delivery_error, kafka_message):
        """
        Callback for message delivery reports
        
        Args:
            delivery_error: Delivery error (if any)
            kafka_message: Delivered message
        """
        if delivery_error:
            logger.error(f"❌ Delivery failed for order: {delivery_error}")
        else:
            # kafka_message.key() contains the orderId if we set it
            order_key = kafka_message.key().decode('utf-8') if kafka_message.key() else "UNKNOWN"
            logger.info(
                f"✓ Delivered [{order_key}] to {kafka_message.topic()} "
                f"[partition={kafka_message.partition()}, offset={kafka_message.offset()}]"
            )
    
    def generate_order(self, order_sequence_number):
        """
        Generate a random order message
        
        Args:
            order_sequence_number: Order identifier
            
        Returns:
            Order dictionary
        """
        available_products = [
            'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones',
            'Webcam', 'USB Cable', 'External Drive', 'Mouse Pad', 'Docking Station'
        ]
        
        # Price ranges for different products
        price_ranges = {
            'Laptop': (799.99, 1999.99),
            'Mouse': (19.99, 79.99),
            'Keyboard': (39.99, 149.99),
            'Monitor': (199.99, 799.99),
            'Headphones': (49.99, 299.99),
            'Webcam': (39.99, 149.99),
            'USB Cable': (5.99, 19.99),
            'External Drive': (59.99, 249.99),
            'Mouse Pad': (9.99, 39.99),
            'Docking Station': (99.99, 299.99)
        }
        
        selected_product = random.choice(available_products)
        minimum_price, maximum_price = price_ranges[selected_product]
        product_price = round(random.uniform(minimum_price, maximum_price), 2)
        
        order_data = {
            'orderId': f"ORD-{order_sequence_number:04d}",
            'product': selected_product,
            'price': product_price,
            'timestamp': int(time.time() * 1000)
        }
        
        return order_data
    
    def produce_order(self, order_data, target_topic='orders'):
        """
        Produce a single order message
        
        Args:
            order_data: Order dictionary
            target_topic: Target Kafka topic
        """
        try:
            # Serialize to JSON
            serialized_order = json.dumps(order_data).encode('utf-8')
            
            self.kafka_producer.produce(
                topic=target_topic,
                key=order_data['orderId'].encode('utf-8'),
                value=serialized_order,
                callback=self.delivery_callback
            )
            self.kafka_producer.poll(0)  # Trigger delivery callbacks
            
        except Exception as e:
            logger.error(f"Failed to produce order {order_data['orderId']}: {e}")
    
    def produce_batch(self, message_count=20, message_interval=1.0, target_topic='orders'):
        """
        Produce a batch of order messages
        
        Args:
            message_count: Number of messages to produce
            message_interval: Delay between messages (seconds)
            target_topic: Target Kafka topic
        """
        logger.info(f"🚀 Starting to produce {message_count} orders to topic '{target_topic}'...")
        logger.info("=" * 70)
        
        for order_index in range(1, message_count + 1):
            order_data = self.generate_order(order_index)
            
            logger.info(
                f"📦 Producing Order {order_index}/{message_count}: "
                f"{order_data['orderId']} | {order_data['product']} | ${order_data['price']:.2f}"
            )
            
            self.produce_order(order_data, target_topic)
            
            if message_interval > 0 and order_index < message_count:
                time.sleep(message_interval)
        
        # Wait for all messages to be delivered
        logger.info("\n⏳ Flushing remaining messages...")
        self.kafka_producer.flush()
        
        logger.info("=" * 70)
        logger.info(f"✅ Successfully produced {message_count} orders!")
    
    def close(self):
        """Close the producer and flush remaining messages"""
        logger.info("Closing producer...")
        self.kafka_producer.flush()


def main():
    """Main execution function"""
    try:
        # Initialize producer
        order_producer = KafkaOrderProducer()
        
        # Produce a batch of orders
        # Adjust message_count and message_interval as needed
        order_producer.produce_batch(
            message_count=30,        # Number of orders
            message_interval=0.5,    # Delay between messages (seconds)
            target_topic='orders'
        )
        
        # Close producer
        order_producer.close()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Producer interrupted by user")
    except Exception as e:
        logger.error(f"Producer error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
