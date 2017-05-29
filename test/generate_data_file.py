"""
@author : Rudresh
@Created on: 19/05/17
"""
import random

with open("xy_data.csv",'w') as f:
    for i in range(1000):
        f.write(str(random.randint(0,100))+","+str(random.randint(0,100))+"\n")
