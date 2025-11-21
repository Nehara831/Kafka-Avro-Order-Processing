#!/usr/bin/env python3
"""
Kafka Utilities
Common utilities for Kafka operations
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_kafka_producer_configuration(kafka_broker_address='localhost:9092', schema_registry_url='http://localhost:8081'):
    """
    Get standard producer configuration
    
    Args:
        kafka_broker_address: Kafka broker address
        schema_registry_url: Schema Registry URL
        
    Returns:
        Dictionary with producer configuration
    """
    return {
        'bootstrap.servers': kafka_broker_address,
        'schema.registry.url': schema_registry_url,
        'acks': 'all',
        'retries': 3,
        'max.in.flight.requests.per.connection': 5,
        'enable.idempotence': True,
        'batch.size': 16384,
        'linger.ms': 10,
        'compression.type': 'snappy',
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000,
    }


def create_kafka_consumer_configuration(
    kafka_broker_address='localhost:9092',
    schema_registry_url='http://localhost:8081',
    consumer_group_id='default-group'
):
    """
    Get standard consumer configuration
    
    Args:
        kafka_broker_address: Kafka broker address
        schema_registry_url: Schema Registry URL
        consumer_group_id: Consumer group ID
        
    Returns:
        Dictionary with consumer configuration
    """
    return {
        'bootstrap.servers': kafka_broker_address,
        'schema.registry.url': schema_registry_url,
        'group.id': consumer_group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 3000,
        'fetch.min.bytes': 1,
        'fetch.max.wait.ms': 500,
    }


def format_order_message_for_display(order_data):
    """
    Format order message for display
    
    Args:
        order_data: Order dictionary
        
    Returns:
        Formatted string
    """
    return (
        f"Order {order_data['orderId']}: "
        f"{order_data['product']} @ ${order_data['price']:.2f}"
    )
