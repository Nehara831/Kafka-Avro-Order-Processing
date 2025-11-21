#!/usr/bin/env python3
"""
Retry Handler
Implements exponential backoff retry logic with error classification
"""

import time
import random
import logging
from typing import Callable, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Exception for temporary failures that should be retried"""
    pass


class PermanentError(Exception):
    """Exception for permanent failures that should not be retried"""
    pass


class RetryHandler:
    """
    Handles retry logic with exponential backoff
    
    Features:
    - Configurable max retries
    - Exponential backoff with jitter
    - Error classification (retryable vs permanent)
    """
    
    def __init__(
        self,
        maximum_retry_attempts: int = 3,
        initial_retry_delay: float = 1.0,
        exponential_backoff_multiplier: float = 2.0,
        maximum_retry_delay: float = 30.0,
        apply_jitter: bool = True
    ):
        """
        Initialize retry handler
        
        Args:
            maximum_retry_attempts: Maximum number of retry attempts
            initial_retry_delay: Initial delay in seconds
            exponential_backoff_multiplier: Multiplier for exponential backoff
            maximum_retry_delay: Maximum delay in seconds
            apply_jitter: Whether to add random jitter to delays
        """
        self.maximum_retry_attempts = maximum_retry_attempts
        self.initial_retry_delay = initial_retry_delay
        self.exponential_backoff_multiplier = exponential_backoff_multiplier
        self.maximum_retry_delay = maximum_retry_delay
        self.apply_jitter = apply_jitter
    
    def calculate_delay(self, retry_attempt_number: int) -> float:
        """
        Calculate delay for a given attempt with exponential backoff
        
        Args:
            retry_attempt_number: Retry attempt number (0-indexed)
            
        Returns:
            Delay in seconds
        """
        # Calculate exponential delay
        calculated_delay = min(
            self.initial_retry_delay * (self.exponential_backoff_multiplier ** retry_attempt_number),
            self.maximum_retry_delay
        )
        
        # Add jitter (±20% randomization)
        if self.apply_jitter:
            jitter_range = calculated_delay * 0.2
            calculated_delay = calculated_delay + random.uniform(-jitter_range, jitter_range)
            calculated_delay = max(0.1, calculated_delay)  # Ensure positive delay
        
        return calculated_delay
    
    def execute_with_retry(
        self,
        target_function: Callable,
        *args,
        error_context: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        Execute a function with retry logic
        
        Args:
            target_function: Function to execute
            *args: Positional arguments for the function
            error_context: Context string for error messages
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function execution
            
        Raises:
            PermanentError: If a permanent error occurs or max retries exceeded
        """
        last_encountered_exception = None
        execution_context = error_context or target_function.__name__
        
        for attempt_number in range(self.maximum_retry_attempts + 1):
            try:
                # Attempt to execute the function
                execution_result = target_function(*args, **kwargs)
                
                # Log success if this was a retry
                if attempt_number > 0:
                    logger.info(f"✓ Retry succeeded for {execution_context} on attempt {attempt_number + 1}")
                
                return execution_result
                
            except PermanentError as permanent_error:
                # Don't retry permanent errors
                logger.error(f"❌ Permanent error in {execution_context}: {permanent_error}")
                raise
                
            except RetryableError as retryable_error:
                last_encountered_exception = retryable_error
                
                if attempt_number < self.maximum_retry_attempts:
                    # Calculate delay and retry
                    retry_delay = self.calculate_delay(attempt_number)
                    logger.warning(
                        f"⚠️  Retryable error in {execution_context} (attempt {attempt_number + 1}/{self.maximum_retry_attempts}): {retryable_error}"
                    )
                    logger.info(f"   Retrying in {retry_delay:.2f} seconds...")
                    time.sleep(retry_delay)
                else:
                    # Max retries exceeded
                    logger.error(
                        f"❌ Max retries ({self.maximum_retry_attempts}) exceeded for {execution_context}: {retryable_error}"
                    )
                    raise PermanentError(f"Max retries exceeded for {execution_context}") from retryable_error
                    
            except Exception as unexpected_error:
                # Unexpected error - treat as permanent
                logger.error(f"❌ Unexpected error in {execution_context}: {unexpected_error}")
                raise PermanentError(f"Unexpected error in {execution_context}: {unexpected_error}") from unexpected_error
        
        # Should never reach here, but just in case
        if last_encountered_exception:
            raise PermanentError(f"Failed after {self.maximum_retry_attempts} retries") from last_encountered_exception
    
    def classify_error(self, exception_to_classify: Exception) -> bool:
        """
        Classify whether an error is retryable
        
        Args:
            exception_to_classify: Exception to classify
            
        Returns:
            True if retryable, False if permanent
        """
        # Already classified errors
        if isinstance(exception_to_classify, RetryableError):
            return True
        if isinstance(exception_to_classify, PermanentError):
            return False
        
        # Common retryable error patterns
        retryable_error_patterns = [
            'timeout',
            'connection',
            'unavailable',
            'temporary',
            'network',
            'socket',
            '503',
            '504',
            '429',  # Rate limit
        ]
        
        error_message_lowercase = str(exception_to_classify).lower()
        
        for error_pattern in retryable_error_patterns:
            if error_pattern in error_message_lowercase:
                return True
        
        # Default to permanent for safety
        return False
    
    def wrap_error(self, original_exception: Exception) -> Exception:
        """
        Wrap an exception as either RetryableError or PermanentError
        
        Args:
            original_exception: Original exception
            
        Returns:
            Wrapped exception
        """
        if isinstance(original_exception, (RetryableError, PermanentError)):
            return original_exception
        
        if self.classify_error(original_exception):
            wrapped_exception = RetryableError(str(original_exception))
            wrapped_exception.__cause__ = original_exception
            return wrapped_exception
        else:
            wrapped_exception = PermanentError(str(original_exception))
            wrapped_exception.__cause__ = original_exception
            return wrapped_exception


def simulate_temporary_failure(attempt_counter: list):
    """Simulate a function that fails temporarily then succeeds"""
    attempt_counter[0] += 1
    
    if attempt_counter[0] < 3:
        raise RetryableError(f"Temporary failure (attempt {attempt_counter[0]})")
    
    return f"Success on attempt {attempt_counter[0]}"


def simulate_permanent_failure():
    """Simulate a function that always fails"""
    raise PermanentError("Invalid data format")


# Example usage
if __name__ == "__main__":
    retry_handler = RetryHandler(
        maximum_retry_attempts=3,
        initial_retry_delay=1.0,
        exponential_backoff_multiplier=2.0,
        apply_jitter=True
    )
    
    # Test 1: Temporary failure that succeeds
    print("\n=== Test 1: Temporary Failure (should succeed) ===")
    attempt_counter = [0]
    try:
        test_result = retry_handler.execute_with_retry(
            simulate_temporary_failure,
            attempt_counter,
            error_context="Test 1"
        )
        print(f"Result: {test_result}")
    except Exception as test_exception:
        print(f"Failed: {test_exception}")
    
    # Test 2: Permanent failure
    print("\n=== Test 2: Permanent Failure (should fail immediately) ===")
    try:
        test_result = retry_handler.execute_with_retry(
            simulate_permanent_failure,
            error_context="Test 2"
        )
        print(f"Result: {test_result}")
    except PermanentError as permanent_test_error:
        print(f"Failed as expected: {permanent_test_error}")
    
    # Test 3: Success on first try
    print("\n=== Test 3: Immediate Success ===")
    try:
        test_result = retry_handler.execute_with_retry(
            lambda: "Success!",
            error_context="Test 3"
        )
        print(f"Result: {test_result}")
    except Exception as test_exception:
        print(f"Failed: {test_exception}")
