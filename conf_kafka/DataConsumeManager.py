"""
@author : Rudresh V
@Created on: 13/04/2017
@Description: Data Consumer Manager - to manage the consumed messages

"""

from concurrent.futures import ProcessPoolExecutor

from conf_kafka.KafkaWrapper import Kafka


class DataConsumeManager:
    """
    Used to consume the messages from the defined topic and partitions in a
    multiprocess fashion, and apply processing logic to a predefined batch of messages
    """

    def __init__(self, partition_count, partition_id, topic_name, group_id, batch_size, initial_offset, process_fn):
        """
        Constructor
        :param partition_count: number of partitions the topic has
        :param topic_name: name of the topic to be subscribed for
        :param group_id: id of the consumer group
        :param batch_size: number of messages to be processed at once
        :param initial_offset: initial offset
        :param process_fn: processing logic for each batch of messages
        """
        self.partition_count = partition_count
        self.partition_id = partition_id
        self.topic_name = topic_name
        self.group_id = group_id
        self.initial_offset = initial_offset
        self.batch_size = batch_size
        self.process_fn = process_fn

    def consume_msg(self, offset):
        """
        Method to consume a batch of messages iteratively and process them
        :param current_partition: partition id
        :return:
        """
        msg_list = []
        # entry = True
        # offset = self.initial_offset

        # s = 0
        # continue to consume messages until the number of messages in a batch matches with the batch size
        # while len(msg_list) == self.batch_size or entry:
        #     s += 1
        #     # print("batch ==> ", s)
        #     entry = False
        kafka_instance = Kafka.get_instance()
        kafka_instance.get_consumer_for_topic(self.topic_name, self.group_id, self.partition_id, offset)
        msg_list = kafka_instance.consume_message(self.topic_name, self.group_id, self.partition_id,
                                                  self.batch_size)
        offset += len(msg_list)
        # if len(msg_list) != 0:
        #     self.process_fn(partition_id=current_partition, data=msg_list)
        # print("------------------done")
        return msg_list, offset

    def multi_partition_consume(self):
        """
        Method to consume messages for all the partitions of the topic
        :return:
        """
        with ProcessPoolExecutor() as executor:
            for partition in range(self.partition_count):
                executor.submit(self.consume_msg, partition)

    def consume_by_partition_id(self, offset):
        """
        Method to consume messages for all the partitions of the topic
        :return:
        """
        return self.consume_msg(offset)
