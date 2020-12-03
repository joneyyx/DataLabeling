#-*- coding: UTF-8 -*-
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark import *
from datetime import timedelta, date

spark = SparkSession.builder.appName("data_Label").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")

def main():
    start_date = date(2019, 11, 22)
    end_date = date(2020, 11, 2)
    hivePath = "wasbs://westlake@saaa01cnprd19909.blob.core.chinacloudapi.cn/westlake_data/ssap/pri_tel/tel_csu_reg/"

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days) + 1):
            yield start_date + timedelta(n)

    for single_date in daterange(start_date, end_date):
        report_date = single_date.strftime("%Y%m%d")
        df = spark.read.options(header="true").csv("wasbs://telematics@dldevblob04datalanding.blob.core.chinacloudapi.cn/csu_reg_info/CSU_REG_{}.csv".format(report_date))
        saveAsTable(df, "pri_tel", "tel_csu_reg", hivePath)


def saveAsTable(df, db, tbl, hivePath):
    df.repartition(30).write.format("parquet") \
        .mode("append") \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)


if __name__ == '__main__':
    main()