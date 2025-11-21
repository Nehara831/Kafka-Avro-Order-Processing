#!/usr/bin/env python3
"""
DLQ Consumer
Monitors and displays messages from the Dead Letter Queue
"""

import json
import logging
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeadLetterQueueConsumer:
    """Consumer for monitoring Dead Letter Queue messages"""
    
    def __init__(
        self,
        kafka_broker_address='localhost:9092',
        consumer_group_id='dlq-monitor-group'
    ):
        """
        Initialize DLQ consumer
        
        Args:
            kafka_broker_address: Kafka broker address
            consumer_group_id: Consumer group ID
        """
        self.kafka_broker_address = kafka_broker_address
        self.consumer_group_id = consumer_group_id
        
        # Consumer configuration
        dlq_consumer_config = {
            'bootstrap.servers': kafka_broker_address,
            'group.id': consumer_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        }
        
        self.dlq_consumer = Consumer(dlq_consumer_config)
        
        # Statistics
        self.dlq_statistics = {
            'total_dlq_messages': 0,
            'errors_by_type': {},
            'errors_by_product': {},
        }
        
        logger.info(f"DLQ Consumer initialized - Group: {consumer_group_id}")
    
    def process_dlq_message(self, dlq_message_data: dict):
        """
        Process and log a DLQ message
        
        Args:
            dlq_message_data: Parsed DLQ message
        """
        self.dlq_statistics['total_dlq_messages'] += 1
        
        # Extract information
        failed_order_id = dlq_message_data.get('original_value', {}).get('orderId', 'UNKNOWN')
        failed_product = dlq_message_data.get('original_value', {}).get('product', 'UNKNOWN')
        failure_error_type = dlq_message_data.get('error_type', 'UNKNOWN')
        failure_error_message = dlq_message_data.get('error_message', 'No error message')
        attempted_retry_count = dlq_message_data.get('retry_count', 0)
        
        # Update statistics
        self.dlq_statistics['errors_by_type'][failure_error_type] = \
            self.dlq_statistics['errors_by_type'].get(failure_error_type, 0) + 1
        self.dlq_statistics['errors_by_product'][failed_product] = \
            self.dlq_statistics['errors_by_product'].get(failed_product, 0) + 1
        
        # Log DLQ message
        logger.warning(
            f"\n{'='*80}\n"
            f"🚨 DLQ MESSAGE #{self.dlq_statistics['total_dlq_messages']}\n"
            f"{'='*80}\n"
            f"Order ID: {failed_order_id}\n"
            f"Product: {failed_product}\n"
            f"Error Type: {failure_error_type}\n"
            f"Error Message: {failure_error_message}\n"
            f"Retry Count: {attempted_retry_count}\n"
            f"Original Topic: {dlq_message_data.get('original_topic', 'N/A')}\n"
            f"Original Partition: {dlq_message_data.get('original_partition', 'N/A')}\n"
            f"Original Offset: {dlq_message_data.get('original_offset', 'N/A')}\n"
            f"{'='*80}"
        )
    
    def monitor_dlq(self, dlq_topics=['orders-dlq']):
        """
        Monitor DLQ topic and display failed messages
        
        Args:
            dlq_topics: List of DLQ topics to monitor
        """
        self.dlq_consumer.subscribe(dlq_topics)
        
        logger.info(f"🔍 Monitoring DLQ topics: {dlq_topics}")
        logger.info("Waiting for messages... (Press Ctrl+C to stop)\n")
        
        try:
            while True:
                kafka_message = self.dlq_consumer.poll(timeout=1.0)
                
                if kafka_message is None:
                    continue
                
                if kafka_message.error():
                    if kafka_message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {kafka_message.error()}")
                        continue
                
                # Parse and process DLQ message
                try:
                    dlq_message_data = json.loads(kafka_message.value().decode('utf-8'))
                    self.process_dlq_message(dlq_message_data)
                    
                except json.JSONDecodeError as parse_error:
                    logger.error(f"Failed to parse DLQ message: {parse_error}")
                except Exception as processing_error:
                    logger.error(f"Error processing DLQ message: {processing_error}")
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  DLQ Monitor interrupted by user")
        
        finally:
            self.print_statistics()
            self.dlq_consumer.close()
    
    def print_statistics(self):
        """Print DLQ statistics"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 DLQ STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Total DLQ Messages: {self.dlq_statistics['total_dlq_messages']}")
        
        if self.dlq_statistics['errors_by_type']:
            logger.info("\nErrors by Type:")
            for error_type, error_count in sorted(
                self.dlq_statistics['errors_by_type'].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                logger.info(f"  {error_type}: {error_count}")
        
        if self.dlq_statistics['errors_by_product']:
            logger.info("\nErrors by Product:")
            for product_name, product_error_count in sorted(
                self.dlq_statistics['errors_by_product'].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                logger.info(f"  {product_name}: {product_error_count}")
        
        logger.info("=" * 80 + "\n")


def main():
    """Main execution function"""
    try:
        # Initialize DLQ consumer
        dead_letter_queue_consumer = DeadLetterQueueConsumer()
        
        # Start monitoring
        dead_letter_queue_consumer.monitor_dlq(dlq_topics=['orders-dlq'])
        
    except Exception as e:
        logger.error(f"DLQ Consumer error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
