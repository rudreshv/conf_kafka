"""
@author : Rudresh V
@Created on: 13/04/2017
"""


class Common:
    def __init__(self):
        pass

    def process_data(self, **kwargs):
        print("partition_id =", kwargs.get('partition_id', None), "\tcount =", len(kwargs.get('data', [])))
