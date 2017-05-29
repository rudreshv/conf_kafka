# """
# @author : Rudresh
# @Created on: 16/05/17
# """
#
# from pywebhdfs.webhdfs import PyWebHdfsClient
#
# # hdfs = PyWebHdfsClient(host='192.168.50.96', port='50070')
# # my_file = 'user/rudresh/data_file_1.csv'
# # s = hdfs.read_file(my_file)
# # s_S = bytes.decode(s).split("\n")
# # for i in s_S:
# #     print(i)
# # print(len(s_S))
# # #
# # exit(1)
# # hdfs = PyWebHdfsClient(host='192.168.50.133', port='50070')
# # my_file = '/dataset/iris_dataset.csv'
# # s = hdfs.read_file(my_file)
# # s_S = bytes.decode(s).split("\n")
# # for i in s_S:
# #     print(i)
# # print(len(s_S))
#
# hdfs = PyWebHdfsClient(host='192.168.50.133', port='50070')
# my_file = 'user/umesh/dataset/iris_dataset.csv'
# s = hdfs.read_file(my_file)
# s_S = bytes.decode(s).split("\n")
# for i in s_S:
#     print(i)
# print(len(s_S))


import argparse

if __name__ == '__main__':
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


    """ Read the arguments """
    parser = argparse.ArgumentParser()
    # parser.add_argument('-c_file', '--config_file', help='config path', required=True)
    parser.add_argument('-k_port', '--kafka_port', help='data source', required=True)
    parser.add_argument('-p_size', '--partition_size', type=str2int, help='data source', required=True)
    parser.add_argument('-p_id', '--partition_id', help='data source', required=True)
    parser.add_argument('-off', '--initial_offset', type=str2int, help='data source', required=True)
    parser.add_argument('-b', '--batch_size', type=str2int, help='data source', required=True)
    parser.add_argument('-k_home', '--kafka_home', help='data source', required=True)
    parser.add_argument('-prod', '--produce_message', type=str2bool, help='data source', required=True)
    parser.add_argument('-cons', '--consume_message', type=str2bool, help='data source', required=True)
    parser.add_argument('-h_host', '--hdfs_host', help='data source', required=True)
    parser.add_argument('-h_port', '--hdfs_port', type=str2int, help='data source', required=True)

    args = parser.parse_args()
    print(args)
