"""
@author : Rudresh V
@Created on: 13/04/2017
@Description: Constants class which defines the property names in config file
"""


class Constants:
    class KafkaConstants:
        KAFKA_HOME_DIR = "kafka_home"
        IP = "kafka_ip"
        PORT = "kafka_port"
        TOPIC_NAME = "topic_name"
        PARTITION_SIZE = "partition_size"
        PARTITION_ID = "partition_id"
        GROUP_ID = "group_id"
        INITIAL_OFFSET = "initial_offset"
        ENCODE_MESSAGE = "encode_message"
        BATCH_SIZE = "batch_size"

    class DataConstants:
        DATA_SOURCE = "data_source"
        RAW_FILE_PATH = "raw_data_file_path"
        HDFS_HOST = "hdfs_host"
        HDFS_PORT = "hdfs_port"

    class Commons:
        PRODUCE_MESSAGE = "produce_message"
        CONSUME_MESSAGE = "consume_message"
