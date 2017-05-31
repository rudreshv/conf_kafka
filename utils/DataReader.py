"""
@author : Rudresh V
@Created on: 13/04/2017
@Decription: Data Reader
"""

from abc import ABCMeta, abstractmethod
import subprocess, random, time
from pywebhdfs.webhdfs import PyWebHdfsClient


class AbstractDataReader(metaclass=ABCMeta):
    """
    Abstract class for reading data
    """

    @abstractmethod
    def read_each_line_data_and_process(self, process_fn):
        pass


class FileDataReader(AbstractDataReader):
    """
    File Data Reader - to read the data from a local file system
    """

    def __init__(self, **kwargs):
        """
        Constructor
        :param file_path: raw data file path
        """
        self.file_path = kwargs["file_path"]

    def read_each_line_data_and_process(self, process_fn):
        """
        Read the data, iterate over all the records and apply a function on each record
        :param process_fn:
        :return:
        """
        with open(self.file_path) as f:
            for line in f:
                process_fn(line)


class HDFSFileDataReader(AbstractDataReader):
    """
    HDFS File Data Reader - to read the data from HDFS
    """

    def __init__(self, **kwargs):
        """
        Constructor
        :param file_path: raw data file path
        """
        self.hdfs_host = kwargs["hdfs_host"]
        self.hdfs_port = kwargs["hdfs_port"]
        self.file_path = kwargs["file_path"]
        print(kwargs)

    def read_each_line_data_and_process(self, process_fn):
        """
        Read the data, iterate over all the records and apply a function on each record
        :param process_fn:
        :return:
        """
        # cat = subprocess.Popen(["hadoop", "fs", "-cat", self.file_path],
        #                        stdout=subprocess.PIPE)
        # for line in cat.stdout:
        #     process_fn(line)

        hdfs = PyWebHdfsClient(host=self.hdfs_host, port=self.hdfs_port)
        my_file = self.file_path
        data_lines = hdfs.read_file(my_file)
        data_lines_string = bytes.decode(data_lines).split("\n")
        for line in data_lines_string[:-1]:
            process_fn(line)
        if len(data_lines_string[-1]) != 0:
            process_fn(data_lines_string[-1])


class RandomDataReader(AbstractDataReader):
    """
    Random Data Reader
    """

    def __init__(self, num_of_records):
        """
        Constructor
        :param num_of_records: num_of_records to generate
        """
        self.num_of_records = num_of_records

    def read_each_line_data_and_process(self, process_fn):
        """
        Read the data, iterate over all the records and apply a function on each record
        :param process_fn:
        :return:
        """
        for _ in self.num_of_records:
            process_fn(random.randint(0, self.num_of_records))
