#-*- coding: UTF-8 -*-
from pyspark import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("vehicle_purchase").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")


