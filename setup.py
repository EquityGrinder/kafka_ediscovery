from setuptools import find_packages, setup

setup(
    name="kafka_ediscovery",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "confluent_kafka",
        "pydantic",
    ],
    entry_points={
        "console_scripts": [
            # Define any command-line scripts here
        ],
    },
)
