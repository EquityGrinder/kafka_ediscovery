import time

from kafka_ediscovery.kafka_api import KafkaAPI
from tests.test_kafka_api import TestData


def produce_test_data():
    # Initialize KafkaAPI
    kafka_api = KafkaAPI()

    # Create a sample data to write
    test_data = TestData()

    # Write the sample data to the Kafka topic
    kafka_api.write_data(test_data)
    kafka_api.logger.info("Sample data written to Kafka topic")

    # Optionally, write multiple messages
    for i in range(1, 5):
        test_data.id = i
        kafka_api.write_data(test_data)
        kafka_api.logger.info(f"Sample data with id {i} written to Kafka topic")
        time.sleep(1)  # Sleep to simulate time between messages


if __name__ == "__main__":
    produce_test_data()
