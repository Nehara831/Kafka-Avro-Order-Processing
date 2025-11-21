#!/usr/bin/env python3
"""
Price Aggregator
Maintains running averages of product prices in real-time
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ProductPriceStatistics:
    """State for aggregating price statistics"""
    order_count: int = 0
    price_sum: float = 0.0
    average_price: float = 0.0
    minimum_price: float = float('inf')
    maximum_price: float = float('-inf')


class PriceAggregator:
    """Real-time price aggregator with running averages"""
    
    def __init__(self):
        """Initialize the aggregator with empty state"""
        self.product_statistics: Dict[str, ProductPriceStatistics] = defaultdict(ProductPriceStatistics)
        self.total_order_count = 0
        self.total_revenue_amount = 0.0
    
    def update(self, product_name: str, product_price: float) -> float:
        """
        Update running average for a product
        
        """
        statistics = self.product_statistics[product_name]
        
        statistics.order_count += 1
        statistics.price_sum += product_price
        statistics.average_price = statistics.price_sum / statistics.order_count
        statistics.minimum_price = min(statistics.minimum_price, product_price)
        statistics.maximum_price = max(statistics.maximum_price, product_price)
        
        self.total_order_count += 1
        self.total_revenue_amount += product_price
        
        return statistics.average_price
    
    def get_average(self, product_name: str) -> float:
        """
        Get current average for a product
        
        """
        return self.product_statistics[product_name].average_price if product_name in self.product_statistics else 0.0
    
    def get_statistics(self, product_name: str) -> Dict:
        """
        Get detailed statistics for a product
        
            
        """
        if product_name not in self.product_statistics:
            return {
                'count': 0,
                'sum': 0.0,
                'average': 0.0,
                'min': 0.0,
                'max': 0.0
            }
        
        statistics = self.product_statistics[product_name]
        return {
            'count': statistics.order_count,
            'sum': round(statistics.price_sum, 2),
            'average': round(statistics.average_price, 2),
            'min': round(statistics.minimum_price, 2),
            'max': round(statistics.maximum_price, 2)
        }
    
    def get_all_statistics(self) -> Dict[str, dict]:
        """
        Get statistics for all products
        
        
        """
        return {
            product_name: self.get_statistics(product_name)
            for product_name in self.product_statistics.keys()
        }
    
    def get_overall_statistics(self) -> Dict:
        """
        Get overall aggregation statistics
        
        
        """
        average_order_value = self.total_revenue_amount / self.total_order_count if self.total_order_count > 0 else 0.0
        
        return {
            'total_orders': self.total_order_count,
            'total_revenue': round(self.total_revenue_amount, 2),
            'average_order_value': round(average_order_value, 2),
            'unique_products': len(self.product_statistics)
        }
    
    def print_summary(self):
        """Print a formatted summary of all statistics"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 PRICE AGGREGATION SUMMARY")
        logger.info("=" * 80)
        
        overall_stats = self.get_overall_statistics()
        logger.info(f"\n🔢 Overall Statistics:")
        logger.info(f"   Total Orders: {overall_stats['total_orders']}")
        logger.info(f"   Total Revenue: ${overall_stats['total_revenue']:.2f}")
        logger.info(f"   Average Order Value: ${overall_stats['average_order_value']:.2f}")
        logger.info(f"   Unique Products: {overall_stats['unique_products']}")
        
        logger.info(f"\n📦 Per-Product Statistics:")
        logger.info("-" * 80)
        logger.info(f"{'Product':<20} {'Count':>8} {'Average':>12} {'Min':>12} {'Max':>12}")
        logger.info("-" * 80)
        
        sorted_products = sorted(
            self.product_statistics.items(),
            key=lambda x: x[1].order_count,
            reverse=True
        )
        
        for product_name, product_stats in sorted_products:
            logger.info(
                f"{product_name:<20} {product_stats.order_count:>8} "
                f"${product_stats.average_price:>11.2f} ${product_stats.minimum_price:>11.2f} ${product_stats.maximum_price:>11.2f}"
            )
        
        logger.info("=" * 80 + "\n")
    
    def reset(self):
        """Reset all aggregation state"""
        self.product_statistics.clear()
        self.total_order_count = 0
        self.total_revenue_amount = 0.0
        logger.info("Aggregator state reset")


if __name__ == "__main__":
    price_aggregator = PriceAggregator()
    
    sample_test_orders = [
        ('Laptop', 999.99),
        ('Mouse', 25.50),
        ('Laptop', 1099.99),
        ('Keyboard', 75.00),
        ('Laptop', 899.99),
        ('Mouse', 29.99),
        ('Monitor', 349.99),
        ('Keyboard', 89.99),
        ('Monitor', 299.99),
        ('Mouse', 19.99),
    ]
    
    for product_name, product_price in sample_test_orders:
        running_average = price_aggregator.update(product_name, product_price)
        print(f"Added {product_name} @ ${product_price:.2f} -> Running avg: ${running_average:.2f}")
    
    price_aggregator.print_summary()
