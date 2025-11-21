#!/usr/bin/env python3
"""
Order Consumer
Consumes order messages from Kafka with Avro deserialization,
performs real-time price aggregation, and handles failures with retry logic
"""

import sys
import json
import logging
from confluent_kafka import Producer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from aggregator.price_aggregator import PriceAggregator
from retry.retry_handler import RetryHandler, RetryableError, PermanentError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AvroOrderConsumer:
    """Kafka consumer for order messages with aggregation and error handling"""
    
    def __init__(
        self,
        kafka_broker_address='localhost:9092',
        schema_registry_url='http://localhost:8081',
        consumer_group_id='order-processor-group'
    ):
        """
        Initialize the order consumer
        
        Args:
            kafka_broker_address: Kafka broker address
            schema_registry_url: Schema Registry URL
            consumer_group_id: Consumer group ID
        """
        self.kafka_broker_address = kafka_broker_address
        self.schema_registry_url = schema_registry_url
        self.consumer_group_id = consumer_group_id
        
        avro_consumer_config = {
            'bootstrap.servers': kafka_broker_address,
            'schema.registry.url': schema_registry_url,
            'group.id': consumer_group_id,
            
            'auto.offset.reset': 'earliest', 
            'enable.auto.commit': False, 
            
            'max.poll.interval.ms': 300000,  
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 3000,
            
            'fetch.min.bytes': 1,
        }
        
        self.avro_consumer = AvroConsumer(avro_consumer_config)
        
        dead_letter_queue_config = {
            'bootstrap.servers': kafka_broker_address,
            'compression.type': 'snappy',
        }
        self.dead_letter_queue_producer = Producer(dead_letter_queue_config)
        
        self.price_aggregator = PriceAggregator()
        self.retry_handler = RetryHandler(
            maximum_retry_attempts=3,
            initial_retry_delay=1.0,
            exponential_backoff_multiplier=2.0,
            maximum_retry_delay=10.0
        )
        
        self.processing_statistics = {
            'processed': 0,
            'failed': 0,
            'retried': 0,
            'sent_to_dlq': 0
        }
        
        logger.info(f"Order Consumer initialized - Group: {consumer_group_id}")
    
    def process_order(self, order_data: dict) -> None:
        """
        Process a single order message
        
        Args:
            order_data: Order dictionary from Kafka
            
        Raises:
            RetryableError: For temporary failures
            PermanentError: For permanent failures
        """
        if not all(key in order_data for key in ['orderId', 'product', 'price']):
            raise PermanentError(f"Invalid order format: missing required fields")
        
        if order_data['price'] <= 0:
            raise PermanentError(f"Invalid price: {order_data['price']}")
        

        import random
        if random.random() < 0.05:
            raise RetryableError("Simulated temporary processing failure")
        
        running_average = self.price_aggregator.update(order_data['product'], order_data['price'])
        
        logger.info(
            f"✓ Processed [{order_data['orderId']}] {order_data['product']} @ ${order_data['price']:.2f} "
            f"| Running Avg: ${running_average:.2f}"
        )
    
    def send_to_dlq(self, kafka_message, processing_error: Exception, attempted_retries: int = 0):
        """
        Send failed message to Dead Letter Queue
        
        """
        try:
            dead_letter_message = {
                'original_topic': kafka_message.topic(),
                'original_partition': kafka_message.partition(),
                'original_offset': kafka_message.offset(),
                'original_key': kafka_message.key().decode('utf-8') if kafka_message.key() else None,
                'original_value': kafka_message.value(),
                'error_message': str(processing_error),
                'error_type': type(processing_error).__name__,
                'retry_count': attempted_retries,
                'failed_at': kafka_message.timestamp()[1] if kafka_message.timestamp()[0] else None,
                'consumer_group': self.consumer_group_id
            }
            
            self.dead_letter_queue_producer.produce(
                topic='orders-dlq',
                key=kafka_message.key(),
                value=json.dumps(dead_letter_message).encode('utf-8')
            )
            self.dead_letter_queue_producer.flush()
            
            order_id = kafka_message.value().get('orderId', 'UNKNOWN')
            logger.warning(f"📮 Sent to DLQ: {order_id} - {processing_error}")
            
            self.processing_statistics['sent_to_dlq'] += 1
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    def consume_messages(self, subscribed_topics=['orders'], maximum_message_count=None):
        """
        Consume and process messages from Kafka topics
        """
        self.avro_consumer.subscribe(subscribed_topics)
        
        logger.info(f"🚀 Starting consumer for topics: {subscribed_topics}")
        logger.info("=" * 80)
        
        processed_message_count = 0
        
        try:
            while True:
                if maximum_message_count and processed_message_count >= maximum_message_count:
                    logger.info(f"Reached max messages limit: {maximum_message_count}")
                    break
                
                kafka_message = self.avro_consumer.poll(timeout=1.0)
                
                if kafka_message is None:
                    continue
                
                if kafka_message.error():
                    if kafka_message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {kafka_message.error()}")
                        continue
                
                try:
                    self.retry_handler.execute_with_retry(
                        self.process_order,
                        kafka_message.value(),
                        error_context=f"Order {kafka_message.value().get('orderId', 'UNKNOWN')}"
                    )
                    
                    self.avro_consumer.commit(message=kafka_message)
                    self.processing_statistics['processed'] += 1
                    processed_message_count += 1
                    
                except PermanentError as e:
                    self.send_to_dlq(kafka_message, e, attempted_retries=self.retry_handler.maximum_retry_attempts)
                    self.avro_consumer.commit(message=kafka_message)
                    self.processing_statistics['failed'] += 1
                    processed_message_count += 1
                    
                except Exception as e:
                    logger.error(f"Unexpected error processing message: {e}")
                    self.send_to_dlq(kafka_message, e, attempted_retries=0)
                    self.avro_consumer.commit(message=kafka_message)
                    self.processing_statistics['failed'] += 1
                    processed_message_count += 1
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Consumer interrupted by user")
        
        finally:
            self.close()
    
    def print_statistics(self):
        """Print consumer statistics"""
        logger.info("\n" + "=" * 80)
        logger.info("=" * 80)
        logger.info(f"Messages Processed: {self.processing_statistics['processed']}")
        logger.info(f"Messages Failed: {self.processing_statistics['failed']}")
        logger.info(f"Sent to DLQ: {self.processing_statistics['sent_to_dlq']}")
        logger.info("=" * 80 + "\n")
        
        self.price_aggregator.print_summary()
    
    def close(self):
        """Close consumer and producer connections"""
        logger.info("\nClosing consumer...")
        
        self.print_statistics()
        
        self.avro_consumer.close()
        self.dead_letter_queue_producer.flush()
        
        logger.info("Consumer closed")


def main():
    try:
        order_consumer = AvroOrderConsumer()
        
        
        order_consumer.consume_messages(
            subscribed_topics=['orders'],
            maximum_message_count=None 
        )
        
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
