import threading
from time import sleep
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from kafka_ediscovery.config import KafkaConfig
from kafka_ediscovery.kafka_api import KafkaAPI


class TestData(BaseModel):
    """
    A sample data class for testing serialization and deserialization.
    """

    id: int = 1
    name: str = "test"
    value: float = 1.0
    is_active: bool = True


@pytest.fixture
def kafka_api():
    return KafkaAPI()


def test_consume_callback(kafka_api):
    """
    Test the consume_callback function to ensure it correctly consumes and processes messages.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """

    def test_function(data):
        print(data)
        assert isinstance(data, TestData)
        assert data.id == 1
        assert data.name == "test"
        assert data.value == 1.0
        assert data.is_active is True

    # Set the topic for both consumer and producer
    topic_name = "test_consume_callback"
    kafka_api.config.producer_topic = topic_name
    kafka_api.config.consumer_topic = topic_name

    # Create the topic if it does not exist
    if not kafka_api.topic_exists(topic_name):
        kafka_api.create_topic(num_partitions=1, replication_factor=1)

    # Start the consume_callback function in a separate thread
    consume_thread = threading.Thread(
        target=kafka_api.consume_callback, args=(TestData, test_function)
    )
    consume_thread.start()

    # Wait for 5 seconds before writing data
    sleep(5)

    # Write data to the topic in another thread
    def write_data():
        data = TestData()
        kafka_api.write_data(data)

    write_thread = threading.Thread(target=write_data)
    write_thread.start()

    # Wait for the threads to complete
    write_thread.join()
    consume_thread.join(timeout=10)  # Timeout to ensure the test doesn't hang


def test_serialize_data(kafka_api):
    """
    Test the serialize_data function to ensure it correctly serializes data.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    # Create a sample data to serialize
    data = TestData()

    # Serialize the data
    serialized_data = kafka_api.serialize_data(data)

    # Assert that the serialized data is not empty
    assert serialized_data is not None

    # Assert that the serialized data is of type str
    assert isinstance(serialized_data, str)


def test_deserialize_data(kafka_api):
    """
    Test the deserialize_data function to ensure it correctly deserializes data.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    # Create a sample serialized data
    test_string = TestData().model_dump_json()
    # Deserialize the data
    deserialized_data = kafka_api.deserialize_data(
        data=test_string, model_class=TestData
    )
    # Assert that the deserialized data is not empty
    assert deserialized_data is not None
    # Assert that the deserialized data is of type TestData
    assert isinstance(deserialized_data, TestData)
    # Assert that the deserialized data has the correct values
    assert deserialized_data.id == 1
    assert deserialized_data.name == "test"
    assert deserialized_data.value == 1.0
    assert deserialized_data.is_active is True


def test_create_topic(kafka_api):
    """
    Test the create_topic function to ensure it correctly creates a Kafka topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    kafka_api.config.producer_topic = "test_consumer"

    # Create the topic
    kafka_api.create_topic(num_partitions=1, replication_factor=1)

    # Verify the topic was created
    topics = kafka_api.admin_client.list_topics().topics
    assert "test_consumer" in topics
    kafka_api.logger.info("test_consumer topic created successfully")


def test_write_data_and_read_data(kafka_api):
    """
    Test the write_data and read_data functions to ensure they correctly write and read data.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    # Create a sample data to write
    data = TestData()
    # Write the data
    kafka_api.write_data(data)

    kafka_api.config.change_consumer_topic("test_producer")

    read_data = kafka_api.read_data(TestData)
    assert read_data == data


def test_read_since(kafka_api):
    """
    Test the read_since function to ensure it correctly reads messages since a given offset.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    test_data = TestData()
    kafka_api.change_consumer_topic(kafka_api.config.producer_topic)

    kafka_api.write_data(test_data)
    kafka_api.write_data(test_data)
    kafka_api.write_data(test_data)
    kafka_api.write_data(test_data)

    positions = kafka_api.get_end_offsets()

    for i in range(10):
        test_data.id += 1
        kafka_api.write_data(test_data)

    records = kafka_api.read_since(test_data, positions)

    assert len(records) == 10


def test_change_consumer_topic(kafka_api):
    """
    Test the change_consumer_topic function to ensure it correctly changes the consumer topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    new_topic = "new_consumer_topic"
    kafka_api.change_consumer_topic(new_topic)
    assert kafka_api.config.consumer_topic == new_topic
    kafka_api.logger.info(f"Consumer topic changed to {new_topic}")


def test_change_producer_topic(kafka_api):
    """
    Test the change_producer_topic function to ensure it correctly changes the producer topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    new_topic = "new_producer_topic"
    kafka_api.change_producer_topic(new_topic)
    assert kafka_api.config.producer_topic == new_topic
    kafka_api.logger.info(f"Producer topic changed to {new_topic}")


def test_subscribe(kafka_api):
    """
    Test the subscribe function to ensure it correctly subscribes to a Kafka topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    new_topic = "new_subscribe_topic"

    # Ensure the topic exists
    if not kafka_api.topic_exists(new_topic):
        kafka_api.create_topic(num_partitions=1, replication_factor=1)

    # Subscribe to the new topic
    kafka_api.subscribe(new_topic)

    # Verify the subscription
    assert kafka_api.config.consumer_topic == new_topic
    kafka_api.logger.info(f"Subscribed to topic {new_topic}")


def test_write_and_read_multiple_data(kafka_api):
    """
    Test writing and reading multiple data entries to ensure they are correctly processed.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    data_list = [
        TestData(id=i, name=f"test_{i}", value=float(i), is_active=bool(i % 2))
        for i in range(5)
    ]
    for data in data_list:
        kafka_api.write_data(data)

    kafka_api.config.change_consumer_topic("test_producer")

    read_data_list = kafka_api.read_data(TestData, n=len(data_list))
    for read_data, data in zip(read_data_list, data_list):
        assert read_data == data
        kafka_api.logger.info(f"Data {data} read successfully")


def test_create_and_verify_topic(kafka_api):
    """
    Test the creation and verification of a Kafka topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    topic_name = "verify_topic_creation"
    kafka_api.config.producer_topic = topic_name
    kafka_api.create_topic(num_partitions=1, replication_factor=1)

    topics = kafka_api.admin_client.list_topics().topics
    assert topic_name in topics
    kafka_api.logger.info(
        f"Topic {topic_name} created and verified successfully"
    )


def test_get_end_offsets(kafka_api):
    """
    Test the get_end_offsets function to ensure it correctly retrieves the end offsets for each partition.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    # Set the same topic for both consumer and producer
    topic_name = "test_topic"
    kafka_api.config.producer_topic = topic_name
    kafka_api.config.consumer_topic = topic_name

    # Check if the topic exists
    topics = kafka_api.admin_client.list_topics().topics
    if topic_name not in topics:
        # Create the topic if it does not exist
        kafka_api.create_topic(num_partitions=1, replication_factor=1)

        # Verify the topic was created
        topics = kafka_api.admin_client.list_topics().topics
        assert topic_name in topics, f"Topic {topic_name} was not created"
        kafka_api.logger.info(f"Topic {topic_name} created successfully")
    else:
        kafka_api.logger.info(f"Topic {topic_name} already exists")

    # Write some initial test data to ensure there are offsets
    test_data = TestData()
    kafka_api.write_data(test_data)
    kafka_api.write_data(test_data)

    # Get the initial end offsets
    initial_end_offsets = kafka_api.get_end_offsets()

    # Verify that the initial end offsets are greater than or equal to 0
    for tp in initial_end_offsets:
        assert (
            tp.offset >= 0
        ), f"Offset for partition {tp.partition} is {tp.offset}"

    # Write additional test data
    kafka_api.write_data(test_data)
    kafka_api.write_data(test_data)

    # Get the new end offsets
    new_end_offsets = kafka_api.get_end_offsets()

    # Verify that the new end offsets are greater than the initial end offsets
    for initial_tp, new_tp in zip(initial_end_offsets, new_end_offsets):
        assert (
            new_tp.offset > initial_tp.offset
        ), f"New offset {new_tp.offset} is not greater than initial offset {initial_tp.offset}"

    kafka_api.logger.info("End offsets updated successfully")


def test_topic_exists(kafka_api):
    """
    Test the topic_exists function to ensure it correctly checks for the existence of a Kafka topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    topic_name = "test_topic_exists"
    kafka_api.config.producer_topic = topic_name

    # Ensure the topic does not exist initially
    if kafka_api.topic_exists(topic_name):
        kafka_api.delete_topic(topic_name)
        # Wait for the topic to be fully deleted
        for _ in range(10):
            if not kafka_api.topic_exists(topic_name):
                break
            sleep(1)
        else:
            raise Exception(f"Topic {topic_name} was not deleted in time")

    assert not kafka_api.topic_exists(
        topic_name
    ), f"Topic {topic_name} should not exist initially"

    # Create the topic
    kafka_api.create_topic(num_partitions=1, replication_factor=1)

    # Verify the topic was created
    assert kafka_api.topic_exists(
        topic_name
    ), f"Topic {topic_name} should exist after creation"

    kafka_api.logger.info(f"Topic {topic_name} existence check passed")

    # Delete the topic
    kafka_api.delete_topic(topic_name)

    # Wait for the topic to be fully deleted
    for _ in range(10):
        if not kafka_api.topic_exists(topic_name):
            break
        sleep(1)
    else:
        raise Exception(f"Topic {topic_name} was not deleted in time")

    # Verify the topic was deleted
    assert not kafka_api.topic_exists(
        topic_name
    ), f"Topic {topic_name} should not exist after deletion"

    kafka_api.logger.info(f"Topic {topic_name} deletion check passed")


def test_delete_topic(kafka_api):
    """
    Test the delete_topic function to ensure it correctly deletes a Kafka topic.

    Args:
        kafka_api (KafkaAPI): The KafkaAPI instance to use for the test.
    """
    topic_name = "test_topic_delete"

    # Create the topic
    kafka_api.config.producer_topic = topic_name
    kafka_api.create_topic(num_partitions=1, replication_factor=1)

    # Verify the topic was created
    assert kafka_api.topic_exists(
        topic_name
    ), f"Topic {topic_name} should exist after creation"

    # Delete the topic
    kafka_api.delete_topic(topic_name)

    # Verify the topic was deleted
    assert not kafka_api.topic_exists(
        topic_name
    ), f"Topic {topic_name} should not exist after deletion"

    kafka_api.logger.info(f"Topic {topic_name} deletion check passed")
