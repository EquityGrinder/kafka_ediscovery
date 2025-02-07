from logging import Logger
from typing import Optional

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
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
    admin_client: Optional[AdminClient] = None
    max_messages: int = 100
    model_config: ConfigDict = {"arbitrary_types_allowed": True}

    def __del__(self) -> None:
        if self.producer:
            self.producer.flush()
        if self.consumer:
            self.consumer.close()

    def __init__(self, consumer=None, producer=None):
        """
        Todo: Check if more configuration options are needed for the producer
        and consumer.
        """
        BaseModel.__init__(self)
        if not self.config:
            self.config = KafkaConfig()

        self.producer = producer or Producer(
            {"bootstrap.servers": self.config.get_bootstrap_servers()}
        )

        self.consumer = consumer or Consumer(
            {
                "bootstrap.servers": self.config.get_bootstrap_servers(),
                "group.id": self.config.group_id,
                "auto.offset.reset": "latest",
            }
        )

        logging_conf = LoggingConfig(log_file=self.config.log_file)
        self.logger = logging_conf.setup_logger(self.config.logger_name)

        self.admin_client = AdminClient(
            {"bootstrap.servers": self.config.get_bootstrap_servers()}
        )

        self.subscribe(self.config.consumer_topic)

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
        return model_class.model_validate_json(data)

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

        ### FIXX FOR ONLY ONE MESSAGE
        return self.deserialize_data(data, data_container)

    def get_end_offsets(self) -> list[TopicPartition]:
        """
        This function should give the last offset for each partition in the topic
        and set the consumer to the newest offset.
        """
        self.consumer.poll(0)
        metadata = self.consumer.list_topics(self.config.consumer_topic)
        partitions = metadata.topics[
            self.config.consumer_topic
        ].partitions.keys()

        end_offsets = []
        for partition in partitions:
            tp = TopicPartition(self.config.consumer_topic, partition)

            # Manually assign the consumer to a partition
            self.consumer.assign([tp])
            # Get the high watermark for the partition
            low_watermark, high_watermark = self.consumer.get_watermark_offsets(
                tp, timeout=1
            )
            self.logger.info(
                f"Partition {partition}: low_watermark={low_watermark}, high_watermark={high_watermark}"
            )

            tp.offset = high_watermark
            end_offsets.append(tp)

        # Assign the consumer to the newest offsets
        self.consumer.assign(end_offsets)
        self.logger.info(f"Consumer set to newest offsets: {end_offsets}")

        # Debug logging to check the assigned offsets
        for tp in end_offsets:
            self.logger.debug(
                f"Assigned offset for partition {tp.partition}: {tp.offset}"
            )

        return end_offsets

    def create_topic(
        self,
        num_partitions: int = 1,
        replication_factor: int = 1,
    ):
        """
        Creates a Kafka topic with the specified number of partitions and replication factor.

        The topic name is taken from the configuration (`self.config.producer_topic`).

        Args:
            num_partitions (int): The number of partitions for the topic.
            replication_factor (int): The replication factor for the topic.

        Returns:
            None
        """
        new_topic = NewTopic(
            topic=self.config.producer_topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        fs = self.admin_client.create_topics([new_topic])

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                self.logger.error(f"Failed to create topic {topic}: {e}")

    def topic_exists(self, topic_name: str) -> bool:
        """
        Checks if a Kafka topic exists.

        Args:
            topic_name: The name of the topic to check.

        Returns:
            bool: True if the topic exists, False otherwise.
        """
        topics = self.admin_client.list_topics().topics
        exists = topic_name in topics
        self.logger.info(f"Topic {topic_name} exists: {exists}")
        return exists

    def consume_callback(self, data_container: BaseModel, callback):
        """
        Continuously consumes messages from the Kafka topic and processes them using the provided callback.

        Args:
            data_container: The data model to deserialize the messages into.
            callback: A function to process the consumed messages.
        """
        self.subscribe(self.config.consumer_topic)
        self.logger.info(
            f"Started consuming messages from topic: {self.config.consumer_topic}"
        )

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                data = msg.value().decode("utf-8")
                self.logger.info(f"Received message: {data}")
                deserialized_data = self.deserialize_data(data, data_container)
                self.logger.info(f"Deserialized data: {deserialized_data}")
                callback(deserialized_data)
        except KeyboardInterrupt:
            self.logger.info("Stopping Kafka consumer...")
        finally:
            self.consumer.close()

    def read_since(
        self, data_container: BaseModel, offsets: list[TopicPartition]
    ) -> list[BaseModel]:

        data_collection = []
        # assign to the offsets
        for tp in offsets:
            if tp.offset != -1:
                self.consumer.assign(
                    [
                        TopicPartition(
                            self.config.consumer_topic, tp.partition, tp.offset
                        )
                    ]
                )

        for i in range(0, self.max_messages):
            msg = self.consumer.poll(1.0)

            if msg is None:
                break
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                break
            else:
                print(
                    "Received message: {}".format(msg.value().decode("utf-8"))
                )
                data = msg.value().decode("utf-8")
                data = self.deserialize_data(data, data_container)
                data_collection.append(data)

        return data_collection

    def change_consumer_topic(self, topic: str):
        """
        Changes the consumer topic to the given topic.

        Args:
            topic: The new topic to subscribe to.
        """
        self.config.change_consumer_topic(topic)
        self.subscribe(topic)

    def change_producer_topic(self, topic: str):
        """
        Changes the producer topic to the given topic.

        Args:
            topic: The new topic to produce to.
        """
        self.config.change_producer_topic(topic)

    def subscribe(self, topic: str):
        """
        Subscribes to the given topic.

        Args:
            topic: The topic to subscribe to.
        """
        self.consumer.subscribe([topic])
        self.consumer.poll(0)
        self.logger.info(f"Subscribed to topic: {topic}")
