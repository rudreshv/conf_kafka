"""
@author : Rudresh V
@Created on: 13/04/2017
@Description: Data Publish Manager - to manage the data publishing job
"""

import time

from .KafkaWrapper import Kafka

from utils import DataReader as data_reader


class DataPublishManager:
    """
    Used to publish the messages to a topic. Encodes the message if required (based on the data source)
    """
    reader_map = {"hdfs": data_reader.HDFSFileDataReader, "file": data_reader.FileDataReader,
                  "random_generator": data_reader.RandomDataReader}
    encode_map = {"hdfs": False, "file": True, "random_generator": True}

    def __init__(self, partition_count, data_source, kwargs, topic_name, process_fn):
        """
        Constructor
        :param data_source: source of the data to be published
        :param data_file_path: raw data file path
        :param topic_name: name of the topic
        :param process_fn: function used to publish the messages
        """
        self.partition_count = partition_count
        self.data_source = data_source
        self.process_fn = process_fn
        self.topic_name = topic_name
        self.data_read = self.reader_map[self.data_source](**kwargs)
        self.kafka_instance = Kafka.get_instance()

    def publish_messages(self):
        """
        Method to read the data and apply a function to each data record
        :return:
        """
        self.data_read.read_each_line_data_and_process(process_fn=self.send_message)
        time.sleep(1)

    def send_message(self, line):
        """
        Method to be applied on each data record
        :param line:

        :return:
        """
        success, msg = self.kafka_instance.produce_message(line, self.topic_name, self.partition_count,
                                                           self.encode_map[self.data_source])
        while not success:
            print("retrying failed message...")
            success, msg = self.kafka_instance.produce_message(line, self.topic_name, self.partition_count,
                                                               self.encode_map[self.data_source])
