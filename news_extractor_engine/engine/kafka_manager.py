import json
import logging
import os
import queue
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import KafkaException, Producer


class KafkaProducerManager:
    """
    Manages a fixed-size pool of Kafka producers with a message queue system.

    This implementation uses a worker thread model where a fixed number of producer threads
    consume messages from a queue, preventing thread resource exhaustion.
    """

    __KAFKA_BOOTSTRAP_SERVERS: str
    __KAFKA_CLIENT_ID_PREFIX: str
    __KAFKA_PRODUCER_TOPIC: str
    __KAFKA_AUTH_ENABLED: bool
    __KAFKA_CONFIG: Dict[str, Any]
    __MAX_PRODUCERS: int = (
        5  # Fixed small number of producers to avoid resource exhaustion
    )
    __QUEUE_MAX_SIZE: int = 10000  # Maximum number of messages to queue before blocking
    __MAX_PRODUCER_RETRIES: int = (
        3  # Maximum number of retries when creating a producer
    )
    __RETRY_BACKOFF_BASE: float = 1.0  # Base backoff time in seconds
    __RETRY_BACKOFF_MAX: float = 30.0  # Maximum backoff time in seconds
    __SHUTDOWN_TIMEOUT: float = 10.0  # Seconds to wait for graceful shutdown

    def __init__(self, num_producers: int = 3):
        """
        Initialize the Kafka producer manager with a fixed pool size and message queue

        Args:
            num_producers: Number of producer threads to create (will not exceed MAX_PRODUCERS)
        """
        # Kafka configuration
        self.__KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.__KAFKA_CLIENT_ID_PREFIX: str = (
            os.getenv("KAFKA_PRODUCER_CLIENT_ID_PREFIX", "news-extractor")
            if os.getenv("KAFKA_PRODUCER_CLIENT_ID_AUTOGENERATED") != "true"
            else "news-extractor-engine"
        )
        self.__KAFKA_PRODUCER_TOPIC: str = os.getenv(
            "KAFKA_PRODUCER_TOPIC", "news_articles"
        )

        # Enhanced Kafka client configuration with thread management settings
        self.__KAFKA_CONFIG: Dict[str, Any] = {
            "bootstrap.servers": self.__KAFKA_BOOTSTRAP_SERVERS,
            "client.id": self.__KAFKA_CLIENT_ID_PREFIX,
        }

        self.__KAFKA_AUTH_ENABLED: bool = (
            os.getenv("KAFKA_AUTH_ENABLED", "false").lower() == "true"
        )
        if self.__KAFKA_AUTH_ENABLED:
            self.__KAFKA_CONFIG["sasl.username"] = os.getenv("KAFKA_AUTH_USERNAME", "")
            self.__KAFKA_CONFIG["sasl.password"] = os.getenv("KAFKA_AUTH_PASSWORD", "")
            self.__KAFKA_CONFIG["sasl.mechanism"] = os.getenv(
                "KAFKA_AUTH_MECHANISM", "PLAIN"
            )
            self.__KAFKA_CONFIG["security.protocol"] = os.getenv(
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"
            )

        # Set up message queue and worker threads
        self.shutdown_event = threading.Event()
        self.message_queue = queue.Queue(maxsize=self.__QUEUE_MAX_SIZE)
        self.producers = []
        self.worker_threads = []
        self.fallback_mode = False

        logging.info("Connecting to Kafka server on %s", self.__KAFKA_BOOTSTRAP_SERVERS)

        # Cap the number of producers to the maximum allowed
        num_producers = min(num_producers, self.__MAX_PRODUCERS)
        logging.info(f"Initializing pool with {num_producers} Kafka producers")

        # Create producer instances
        for i in range(num_producers):
            try:
                worker_config = self.__KAFKA_CONFIG.copy()
                worker_config["client.id"] = (
                    f"{self.__KAFKA_CLIENT_ID_PREFIX}-worker-{i}"
                )

                # Create producer with retries
                producer = self._create_producer_with_retry(worker_config)
                if producer:
                    self.producers.append(producer)

                    # Create and start worker thread for this producer
                    worker = threading.Thread(
                        target=self._worker_thread,
                        args=(producer, i),
                        name=f"kafka-worker-{i}",
                        daemon=True,
                    )
                    worker.start()
                    self.worker_threads.append(worker)
                    logging.info(f"Started Kafka worker thread {i}")
            except Exception as e:
                logging.error(f"Failed to initialize producer {i}: {str(e)}")

        if not self.worker_threads:
            logging.critical(
                "CRITICAL: Could not create any Kafka producers. Messages will be lost!"
            )
            self.fallback_mode = True
        else:
            logging.info(
                f"Successfully started {len(self.worker_threads)} Kafka worker threads"
            )

    def _worker_thread(self, producer: Producer, worker_id: int):
        """
        Worker thread that processes messages from the queue

        Args:
            producer: The Kafka producer instance for this worker
            worker_id: Worker thread ID for logging
        """
        logging.info(f"Kafka worker {worker_id} started")

        while not self.shutdown_event.is_set():
            try:
                # Get message with timeout to allow checking shutdown event
                try:
                    message = self.message_queue.get(timeout=0.5)
                except queue.Empty:
                    # Poll for any callbacks even when no new messages
                    producer.poll(0)
                    continue

                try:
                    key, value, callback = message
                    producer.produce(
                        self.__KAFKA_PRODUCER_TOPIC,
                        key=key,
                        value=value,
                        callback=callback,
                    )
                    # Poll to handle callbacks
                    producer.poll(0)
                except Exception as e:
                    logging.error(
                        f"Worker {worker_id} failed to produce message: {str(e)}"
                    )
                finally:
                    # Always mark task as done
                    self.message_queue.task_done()

            except Exception as e:
                logging.error(f"Error in Kafka worker {worker_id}: {str(e)}")

        logging.info(f"Kafka worker {worker_id} shutting down")
        try:
            # Flush on shutdown
            producer.flush(timeout=2.0)
        except Exception as e:
            logging.error(f"Error flushing producer in worker {worker_id}: {str(e)}")

    def _create_producer_with_retry(self, config: Dict[str, Any]) -> Optional[Producer]:
        """
        Attempt to create a producer with retries and exponential backoff

        Args:
            config: Kafka producer configuration

        Returns:
            Producer instance if successful, None otherwise
        """
        retry_count = 0
        while retry_count < self.__MAX_PRODUCER_RETRIES:
            try:
                return Producer(config)
            except KafkaException as e:
                retry_count += 1

                # Calculate backoff time with exponential backoff
                backoff_time = min(
                    self.__RETRY_BACKOFF_MAX,
                    self.__RETRY_BACKOFF_BASE * (2 ** (retry_count - 1)),
                )

                if retry_count < self.__MAX_PRODUCER_RETRIES:
                    logging.warning(
                        f"Producer creation attempt {retry_count} failed: {str(e)}. Retrying in {backoff_time:.1f}s..."
                    )
                    time.sleep(backoff_time)
                else:
                    logging.error(
                        f"Failed to create producer after {self.__MAX_PRODUCER_RETRIES} attempts: {str(e)}"
                    )
            except Exception as e:
                logging.error(f"Unexpected error creating producer: {str(e)}")
                retry_count += 1
                if retry_count < self.__MAX_PRODUCER_RETRIES:
                    time.sleep(1)  # Simple delay for unexpected errors
                else:
                    break

        return None

    def publish_message(self, key: str, value: dict) -> bool:
        """
        Publish a message to Kafka via the queue

        Args:
            key: Message key
            value: Message value (will be converted to JSON)

        Returns:
            True if message was queued successfully, False otherwise
        """
        if self.fallback_mode or self.shutdown_event.is_set():
            logging.warning(
                f"Kafka manager is in fallback mode or shutting down. Message not sent: {key}"
            )
            return False

        try:
            encoded_value = json.dumps(value).encode("utf-8")

            # Set up delivery tracking
            delivery_event = threading.Event()
            delivery_result = {"success": False}

            def delivery_callback(err, msg):
                if err:
                    logging.error(f"Message delivery failed: {err}")
                else:
                    logging.debug(
                        f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
                    )
                    delivery_result["success"] = True
                delivery_event.set()

            # Add to queue with timeout
            try:
                self.message_queue.put(
                    (key, encoded_value, delivery_callback), timeout=2.0
                )
                return True
            except queue.Full:
                logging.error(f"Message queue is full, message will not be sent: {key}")
                return False

        except Exception as e:
            logging.error(f"Failed to prepare message for Kafka: {str(e)}")
            return False

    def flush_all(self):
        """Flush all producers"""
        if not self.producers:
            return

        logging.info("Flushing all Kafka producers")
        # First make sure queue is empty
        if not self.message_queue.empty():
            try:
                # Wait for queue to drain with timeout
                self.message_queue.join()
            except Exception as e:
                logging.error(f"Error waiting for message queue to drain: {str(e)}")

        # Then flush each producer
        for i, producer in enumerate(self.producers):
            try:
                producer.flush(timeout=2.0)
            except Exception as e:
                logging.error(f"Error flushing producer {i}: {str(e)}")

    def close_all(self):
        """Close all producers and stop worker threads"""
        if self.shutdown_event.is_set():
            return  # Already shutting down

        logging.info("Closing all Kafka producers")

        # Signal threads to stop
        self.shutdown_event.set()

        # Wait for worker threads to finish with timeout
        for i, thread in enumerate(self.worker_threads):
            try:
                thread.join(timeout=self.__SHUTDOWN_TIMEOUT / len(self.worker_threads))
                if thread.is_alive():
                    logging.warning(
                        f"Kafka worker thread {i} did not terminate gracefully"
                    )
            except Exception as e:
                logging.error(f"Error joining worker thread {i}: {str(e)}")

        # Clear thread list
        self.worker_threads.clear()

        # Clean up producers
        for producer in self.producers:
            # No need to call flush here as workers should have flushed on shutdown
            pass

        # Clear producer list
        self.producers.clear()

        logging.info("All Kafka producers have been closed")

    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the queue and producer pool

        Returns:
            Dictionary with queue statistics
        """
        return {
            "queue_size": self.message_queue.qsize(),
            "queue_full": self.message_queue.full(),
            "active_workers": len(self.worker_threads),
            "producers": len(self.producers),
            "fallback_mode": self.fallback_mode,
        }
