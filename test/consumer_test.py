"""
@author : Rudresh
@Created on: 31/05/17
"""

import json

from Constants import Constants as constants
from KafkaWrapper import Kafka

from conf_kafka.DataConsumeManager import DataConsumeManager


def process_fn():
    pass


class Consumer:
    def __init__(self):
        self.props = None
        self.config_file_path = "/Users/rudresh/git/conf_kafka/config/config_rudresh.json"
        self.load_properties()
        kafka_instance = Kafka()
        kafka_instance.setup(self.props[constants.KafkaConstants.KAFKA_HOME_DIR],
                             self.props[constants.KafkaConstants.IP],
                             self.props[constants.KafkaConstants.PORT])
        self.data_consumer = DataConsumeManager(self.props[constants.KafkaConstants.PARTITION_SIZE],
                                                self.props[constants.KafkaConstants.PARTITION_ID],
                                                self.props[constants.KafkaConstants.TOPIC_NAME],
                                                self.props[constants.KafkaConstants.GROUP_ID],
                                                self.props[constants.KafkaConstants.BATCH_SIZE],
                                                self.props[constants.KafkaConstants.INITIAL_OFFSET],
                                                process_fn)
        self.offset = self.props[constants.KafkaConstants.INITIAL_OFFSET]

    def load_properties(self):
        self.props = json.load(open(self.config_file_path))

    def next(self):
        msg_list, self.offset = self.data_consumer.consume_by_partition_id(self.offset)
        return msg_list


if __name__ == '__main__':
    cons = Consumer()
    data = None
    while data is None or len(data) == cons.props[constants.KafkaConstants.BATCH_SIZE]:
        data = cons.next()
        print(len(data))
