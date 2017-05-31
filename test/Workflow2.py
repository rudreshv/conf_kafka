"""
@author : Rudresh V
@Created on: 13/04/2017
"""

import argparse
import ast
import os
import time

from conf_kafka.Constants import Constants as constants
from conf_kafka.DataConsumeManager import DataConsumeManager
from conf_kafka.DataPublishManager import DataPublishManager
from conf_kafka.KafkaWrapper import Kafka

from conf_kafka.CommonFunctions import Common


class Workflow:
    def __init__(self, props):
        self.props = props

    # def load_properties(self):
    #     self.props = json.load(open(self.config_file_path))

    def run(self, ):
        # self.load_properties()
        kafka_instance = Kafka()
        kafka_instance.setup(self.props[constants.KafkaConstants.KAFKA_HOME_DIR],
                             self.props[constants.KafkaConstants.IP],
                             self.props[constants.KafkaConstants.PORT])

        if self.props[constants.Commons.PRODUCE_MESSAGE]:
            start_time = time.time()
            # publish the messages

            kwargs = {"hdfs_host": self.props[constants.DataConstants.HDFS_HOST],
                      "hdfs_port": self.props[constants.DataConstants.HDFS_PORT],
                      "file_path": self.props[constants.DataConstants.RAW_FILE_PATH]}

            data_publisher = DataPublishManager(self.props[constants.KafkaConstants.PARTITION_SIZE],
                                                self.props[constants.DataConstants.DATA_SOURCE],
                                                kwargs,
                                                self.props[constants.KafkaConstants.TOPIC_NAME], None)
            data_publisher.publish_messages()
            # kafka_instance.producer.flush()
            end_time = time.time()
            print("time taken to publish messages", end_time - start_time, "seconds")

        if self.props[constants.Commons.CONSUME_MESSAGE]:
            start_time = time.time()
            # function which processes each batch of data
            data_process = Common()
            process_fn = data_process.process_data

            #  consumes the messages
            data_consumer = DataConsumeManager(self.props[constants.KafkaConstants.PARTITION_SIZE],
                                               self.props[constants.KafkaConstants.PARTITION_ID],
                                               self.props[constants.KafkaConstants.TOPIC_NAME],
                                               self.props[constants.KafkaConstants.GROUP_ID],
                                               self.props[constants.KafkaConstants.BATCH_SIZE],
                                               self.props[constants.KafkaConstants.INITIAL_OFFSET],
                                               process_fn)
            # data_consumer.multi_partition_consume()
            offset = self.props[constants.KafkaConstants.INITIAL_OFFSET]
            consume = True
            while consume:
                msg_list, offset = data_consumer.consume_by_partition_id(offset)
                if len(msg_list) < self.props[constants.KafkaConstants.BATCH_SIZE]:
                    consume = False
                print(len(msg_list), msg_list)
            end_time = time.time()
            print("time taken to consume messages", end_time - start_time, "seconds")


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def str2int(v):
    try:
        return int(v)
    except:
        raise argparse.ArgumentTypeError('Integer value expected.')


if __name__ == '__main__':
    """ Read the arguments """
    CONFIG = ast.literal_eval(os.environ["KAFKA_PRODUCER_CONFIG"])
    parser = argparse.ArgumentParser()

    # example run: python3.5 Workflow.py  -ds file -dp "/Users/rudresh/PycharmProjects/ril_model/csv_data/data_file_1.csv" -t "topic_1"  -k_ip "localhost" -k_port 9092 -p_size 10 -p_id 0 -g_id 4 -off 0 -b 1000 -k_home "/Users/rudresh/Documents/Personal/Softwares/kafka_2.11-0.10.2.0" -prod True -cons true -h_host 192.168.50.96 -h_port 50070

    parser.add_argument('-ds', '--data_source', help='data source', default=CONFIG["DATA_SOURCE"])
    parser.add_argument('-dp', '--raw_data_file_path', help='raw data file path', default=CONFIG["RAW_DATA_FILE_PATH"])
    parser.add_argument('-t', '--topic_name', help='topic name', default=CONFIG["TOPIC_NAME"])
    parser.add_argument('-k_ip', '--kafka_ip', help='kafka host ip ', default=CONFIG["KAFKA_IP"])
    parser.add_argument('-k_port', '--kafka_port', type=str2int, help='kafka port', default=CONFIG["KAFKA_PORT"])
    parser.add_argument('-p_size', '--partition_size', type=str2int, help='partition size',
                        default=CONFIG["PARTITION_SIZE"])
    parser.add_argument('-p_id', '--partition_id', type=str2int, help='partition id', default=CONFIG["PARTITION_ID"])
    parser.add_argument('-g_id', '--group_id', help='group id', default=CONFIG["GROUP_ID"])
    parser.add_argument('-off', '--initial_offset', type=str2int, help='initial offset',
                        default=CONFIG["INITIAL_OFFSET"])
    parser.add_argument('-b', '--batch_size', type=str2int, help='batch size', default=CONFIG["BATCH_SIZE"])
    parser.add_argument('-k_home', '--kafka_home', help='kafka home', required=False, default=CONFIG["KAFKA_HOME"])
    parser.add_argument('-prod', '--produce_message', type=str2bool, help='produce message',
                        default=CONFIG["PRODUCE_MESSAGE"])
    parser.add_argument('-cons', '--consume_message', type=str2bool, help='consume message',
                        default=CONFIG["CONSUME_MESSAGE"])
    parser.add_argument('-dfs_host', '--hdfs_host', help='hdfs host', default=CONFIG["HDFS_HOST"])
    parser.add_argument('-dfs_port', '--hdfs_port', type=str2int, help='hdfs port', default=CONFIG["HDFS_PORT"])
    props = vars(parser.parse_args())

    workflow = Workflow(props)
    workflow.run()
