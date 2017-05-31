"""
@author : Rudresh V
@Created on: 13/04/2017
@Description: Wrapper for kafka connection and api functions
"""

import logging
import subprocess

import confluent_kafka

from utils.Singleton import Singleton

log = logging.getLogger(__name__)


class Kafka(metaclass=Singleton):
    """
    Wrapper for Kafka connection and api functions
    """

    def __init__(self):
        self.kafka_home = None
        self.ip = None
        self.port = None
        self.is_setup = False
        self.bootstrap_servers = None
        self.producer = None
        self.consumer_dict = None
        self.topic_dict = None
        self.max_msg_count = 10000000

    def setup(self, kafka_home, ip, port):
        """
        Setup the kafka connection
        :param kafka_home: kafka_home dir
        :param ip: ip
        :param port: port
        :return:
        """
        self.kafka_home = kafka_home
        self.ip = ip
        self.port = port
        self.bootstrap_servers = str(self.ip) + ":" + str(self.port)
        self.producer = None
        self.consumer_dict = dict()
        self.topic_dict = dict()
        self.is_setup = True
        return self

    @staticmethod
    def get_instance():
        """
        Method to get the kafka connection instance
        :return: returns the kafka connection instance if setup is done once
        """
        kafka_instance = Kafka()
        if kafka_instance.is_setup:
            return kafka_instance
        else:
            raise Exception("Setup the kafka instance first...")

    def send_message(self, line, topic, encode=False):
        self.produce_message(line, topic, encode)

    def get_producer_for_topic(self):
        """
        Method to instantiate the kafka producer
        :return: producer instance
        """
        try:
            log.info("Fetching producer...")
            if self.producer is not None:
                return self.producer
            conf = {'bootstrap.servers': self.bootstrap_servers,
                    'queue.buffering.max.messages': self.max_msg_count,
                    # 'batch.num.messages': 0.1 * self.max_msg_count,
                    # 'queue.buffering.max.ms': 5000
                    }
            producer = confluent_kafka.Producer(**conf)
            self.producer = producer
        except Exception as e:
            log.error("Error while setting up the producer")
            raise e
        return producer

    def get_consumer_for_topic(self, topic_name, group_id, partition, offset=None):
        """
        Method to instantiate the kafka consumer for the given topic, consumer group and partition
        :param topic_name: topic name
        :param group_id: consumer group id
        :param partition: partition id
        :return: consumer instance
        """
        try:
            log.info("Fetching consumer for topic: " + topic_name)
            if topic_name + "_" + str(partition) in self.consumer_dict:
                return self.consumer_dict[topic_name + "_" + str(partition)]
            conf = {'bootstrap.servers': self.bootstrap_servers,
                    'group.id': group_id,
                    # 'session.timeout.ms': 1000,
                    'default.topic.config': {
                        'auto.offset.reset': 'earliest'
                    }
                    }
            consumer = confluent_kafka.Consumer(**conf)

            if offset is None:
                tp = confluent_kafka.TopicPartition(topic_name, partition)
            else:
                tp = confluent_kafka.TopicPartition(topic_name, partition, offset)
            consumer.assign([tp])
            self.consumer_dict[topic_name + "_" + str(partition)] = consumer
        except Exception as e:
            print(e)
            log.error("Error while setting up the consumer for topic: " + topic_name)
            raise e
        return consumer

    def produce_message(self, message, topic_name, partition_size, encode=True):
        """
        Method to produce/publish a message to topic
        :param message: message to be published
        :param topic_name: topic name to be published for
        :param partition_size: size of the partitions if topic is not created
        :param encode: boolean - encode the message or not
        :return:
        """
        try:
            if topic_name not in self.topic_dict:
                # self.create_topic(topic_name, partition_size)
                self.topic_dict[topic_name] = True
            if encode:
                msg = str.encode(message)
            else:
                msg = message
            if self.producer is None:
                self.producer = self.get_producer_for_topic()
            self.producer.produce(topic_name, msg)
            self.producer.poll(0)
        except BufferError:
            return False, msg
        except Exception as e:
            print(e)
            exit(1)

        return True, msg

    def consume_message(self, topic_name, group_id, partition, batch_size=0):
        """
        Method to consume a batch of messages
        :param topic_name: topic name
        :param group_id: id of consumer group
        :param partition: partition id
        :param batch_size: batch size
        :return: batch of messages
        """
        msg_list = []
        try:
            count = 0
            if topic_name + "_" + str(partition) not in self.consumer_dict:
                self.consumer_dict[topic_name + "_" + str(partition)] = self.get_consumer_for_topic(topic_name,
                                                                                                    group_id, partition)
            consumer = self.consumer_dict[topic_name + "_" + str(partition)]
            while True:
                msg = consumer.poll()
                if msg is not None and msg.error() is not None and msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    break
                if msg:
                    count += 1
                    msg_list.append(bytes.decode(msg.value()).replace("\n", ""))
                if count >= batch_size:
                    break
            return msg_list
        except Exception as e:
            print(e)
            log.error(
                "Error while consuming the message to topic:" + topic_name + " group_id:" + str(
                    group_id) + " partition:" + str(partition))
            raise e

    def create_topic(self, topic_name, partition_size=1):
        arg_list = "ssh rudresh@192.168.50.96 " + self.kafka_home + "/bin/kafka-topics.sh --create --zookeeper " + self.ip + \
                   ":2181 --replication-factor 1 --partitions " + str(partition_size) + \
                   " --topic " + topic_name
        out = self.run_command(arg_list.split(" "))
        return bytes.decode(out).__contains__("exists")

    @staticmethod
    def run_command(args):
        arg_list = [arg for arg in args]
        out = subprocess.Popen(arg_list, stdout=subprocess.PIPE)
        text = out.communicate()[0]
        return text
