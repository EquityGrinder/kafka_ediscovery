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
    config = KafkaConfig()
    return KafkaAPI()


def test_serialize_data(kafka_api):
    # TODO: Add test case for serialize_data method
    # Create a sample data to serialize
    data = TestData()

    # Serialize the data
    serialized_data = kafka_api.serialize_data(data)

    # Assert that the serialized data is not empty
    assert serialized_data is not None

    # Assert that the serialized data is of type bytes
    assert isinstance(serialized_data, str)


def test_deserialize_data(kafka_api):
    # TODO: Add test case for deserialize_data method
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


def test_write_data_and_read_data(kafka_api):
    # TODO: Add test case for write_data method
    # Create a sample data to write
    data = TestData()
    # Write the data
    kafka_api.write_data(data)

    kafka_api.config.change_consumer_topic("test_producer")

    assert kafka_api.read_data(data) == data
