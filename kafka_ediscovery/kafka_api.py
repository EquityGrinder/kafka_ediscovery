from logging import Logger
from typing import Optional

from confluent_kafka import Consumer, Producer, TopicPartition
from pydantic import BaseModel, ConfigDict

from kafka_ediscovery.config import KafkaConfig, LoggingConfig

"""
This module communicates with Kafka to read and write data from a given topic.

It connects automatically to the configured cluster. It provides convenience
methods to serialize and deserialize data to and from the Kafka topic.

It is implemented in a Pydantic-style object.

It uses Confluent Kafka to connect and interact with the Kafka cluster.
In the config module, the KafkaConfig class is defined,
which is used to configure the connection to the Kafka cluster.
"""


class KafkaAPI(BaseModel):
    config: Optional[KafkaConfig] = None
    producer: Optional[Producer] = None
    consumer: Optional[Consumer] = None
    logger: Optional[Logger] = None

    model_config: ConfigDict = {"arbitrary_types_allowed": True}

    def __init__(self):
        """
        Todo: Check if more configuration options are needed for the producer
        and consumer.
        """
        BaseModel.__init__(self)
        if not self.config:
            self.config = KafkaConfig()

        self.producer = Producer(
            {"bootstrap.servers": self.config.get_bootstrap_servers()}
        )

        self.consumer = Consumer(
            {
                "bootstrap.servers": self.config.get_bootstrap_servers(),
                "group.id": self.config.group_id,
                "auto.offset.reset": "earliest",
            }
        )

        logging_conf = LoggingConfig(log_file=self.config.log_file)
        self.logger = logging_conf.setup_logger(self.config.logger_name)


    def serialize_data(self, data: BaseModel):
        """
        Checks if data is a pydantic.BaseModel if yes returns the json string

        args
            data : pydantic.BaseModel
        """

        serialized_data = data.model_dump_json()
        self.logger.info(f"Serialized data: {serialized_data}")
        return data.model_dump_json()

    def deserialize_data(
        self,
        data: str,
        model_class: BaseModel,
    ) -> BaseModel:
        """
        Deserializes the data and returns the pydantic model

        Args:
            data: Serialized data
            model_class: Pydantic model class to deserialize into
        """
        self.logger.info(f"Deserializing data: {data}")
        return model_class.parse_raw(data)

    def write_data(self, data: BaseModel):
        """
        Writes the given data to the Kafka topic.

        Args:
            data: The data to be written.

        Returns:
            None

        Todo:
            - flushing does not properly work
        """

        serialized_data = self.serialize_data(data)
        self.producer.produce(self.config.producer_topic, value=serialized_data)
        self.producer.flush()
        self.logger.info(
            f"Data written to Kafka topic: {self.config.producer_topic}"
        )

    def read_data(self, data_container: BaseModel, n: int = 1):
        """
        Reads data from the Kafka consumer and returns the deserialized data.
        It reads the last n messages.

        This function is clearly not in the spirit of a stream processing
        system.
        It is more of a convenience function to read the last n messages.

        Args:
            data_container (BaseModel):The data container object
                                        used for deserialization.
            n (int): The number of messages to read.
        Returns:
            The deserialized data.


        Todo :: Check which part of the code can be moved to a different method
        and is not necessary for the read_data method to be executed every time
        """
        self.logger.info(
            "Consume last "
            + str(n)
            + " messages from topic: "
            + self.config.consumer_topic
        )
        # Get the number of partitions for the topic
        metadata = self.consumer.list_topics(self.config.consumer_topic)
        num_partitions = len(
            metadata.topics[self.config.consumer_topic].partitions.keys()
        )

        # Create a list of TopicPartitions
        tps = [
            TopicPartition(self.config.consumer_topic, i)
            for i in range(num_partitions)
        ]

        # Get the last offsets for each partition
        end_offsets = [
            self.consumer.get_watermark_offsets(partition)[1]
            for partition in tps
        ]
        self.logger.debug(f"End offsets: {end_offsets}")

        # Create a list of TopicPartitions with the correct start offset
        tps = [
            TopicPartition(
                self.config.consumer_topic, i, max(0, end_offsets[i] - n)
            )
            for i in range(0, len(end_offsets))
        ]

        # Assign the consumer to the TopicPartitions
        self.consumer.assign(tps)

        for i in range(n):
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            else:
                data = msg.value().decode("utf-8")
                self.logger.info(f"Read data: {data}")

        self.consumer.close()

        ### FIXX FOR ONLY ONE MESSAGE
        return self.deserialize_data(data, data_container)

    def change_consumer_topic(self, topic: str):
        """
        Changes the consumer topic to the given topic.

        Args:
            topic: The new topic to subscribe to.
        """
        self.config.change_consumer_topic(topic)

    def change_producer_topic(self, topic: str):
        """
        Changes the producer topic to the given topic.

        Args:
            topic: The new topic to produce to.
        """
        self.config.change_producer_topic(topic)
