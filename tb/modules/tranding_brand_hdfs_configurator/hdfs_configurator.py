'''

    File name: hdfs_configurator.py
    Author: David Cuesta
    Python Version: 3.6
'''
########################################################################################################################
# IMPORTS
########################################################################################################################
import pyhdfs
########################################################################################################################
hdfs = pyhdfs.HdfsClient(hosts='hdfs-namenode:50070')
hdfs.copy_from_local("../..//Dataset/positive.txt","hdfs://hdfs-namenode:50070/positive.txt")
hdfs.copy_from_local("../..//Dataset/negative.txt","hdfs://hdfs-namenode:50070/negative.txt")
hdfs.mkdirs("/spark",permission="777")


