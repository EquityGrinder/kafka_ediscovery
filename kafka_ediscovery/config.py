import logging
from typing import Union

from pydantic import BaseModel

"""
This module contains the MilvusConfig class which handles configurations for connecting to a Milvus server.

The MilvusConfig class uses the Pydantic library to define the configuration model. It includes attributes for the host and port of the Milvus server, with default values provided.

Typical usage example:

    config = MilvusConfig(host="my.milvus.server", port=12345)
"""


class KafkaConfig(BaseModel):
    """
    Configuration class for Kafka settings.

    This class represents the configuration settings for Kafka, including the host address, port number,
    consumer group ID, consumer topic name, and producer topic name.

    Attributes:
        host (str): The Kafka host address.
        port (int): The Kafka port number.
        group_id (str): The consumer group ID.
        consumer_topic (str): The consumer topic name.
        producer_topic (str): The producer topic name.
    """

    host: str = "kafka.dev.io"
    port: int = 9093

    group_id: str = "test"
    consumer_topic: str = "test_consumer"
    producer_topic: str = "test_producer"

    def get_bootstrap_servers(self):
        """
        Get the bootstrap servers in the format of "host:port".

        Returns:
            str: The bootstrap servers.
        """
        return f"{self.host}:{self.port}"

    def change_consumer_topic(self, topic: str):
        """
        Change the consumer topic.

        Args:
            topic (str): The new consumer topic name.

        Todo:
            - Check the existence of the topic before changing.
        """
        self.consumer_topic = topic

    def change_producer_topic(self, topic: str):
        """
        Change the producer topic.

        Args:
            topic (str): The new consumer topic name.

        Todo:
            - Check the existence of the topic before changing.
        """
        self.producer_topic = topic


class MilvusConfig(BaseModel):
    """
    A class handling configurations from Milvus.

    This class represents the configuration settings for connecting to a Milvus server.
    It provides default values for the host and port, which can be overridden if needed.

    Attributes:
        host (str): The host address of the Milvus server. Defaults to "milvus.dev.io".
        port (int): The port number of the Milvus server. Defaults to 19530.
    """

    host: str = "milvus.dev.io"
    port: int = 19530


class LoggingConfig(BaseModel):
    """
    A class handling configurations for logging.

    This class represents the configuration settings for logging, including the log file path, log level, and log format.

    Attributes:
        log_file (str): The path to the log file.
        log_level (int): The log level.
        log_format (str): The log format string.
    """

    log_file: str = "logs/main.log"
    log_level: int = logging.DEBUG
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def setup_logger(self, name: str):
        """To setup as many loggers as you want"""

        handler = logging.FileHandler(self.log_file)
        formatter = logging.Formatter(self.log_format)
        handler.setFormatter(formatter)

        logger = logging.getLogger(name)
        logger.setLevel(self.log_level)
        logger.addHandler(handler)

        return logger
