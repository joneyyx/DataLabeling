#-*- coding: UTF-8 -*-
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark import *
import pandas as pd
import numpy as np
import requests
import time,sys
from time import sleep
from datetime import datetime, timedelta
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

spark = SparkSession.builder.appName("GPS_Fill").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")

def main():
    yesterday = datetime.now()
    yesterday = datetime.now() - timedelta(1)
    # start_date = datetime.strftime(yesterday, '%Y%m%d')
    df = spark.sql("select engine_serial_number, longitude, latitude, report_date, province, roadname,engine_name from data_labeled.hblabeled_data where report_date = '{}'".format(start_date))

    master = spark.sql("select esn, emission_level, plant from data_labeled.esn_master_detail")
    filterNSVIDF = df.join(master, df.engine_serial_number == master.esn, how='left').filter(master.emission_level == 'NSVI').filter(master.plant == 'DCEC')

    gpsDF = filterNSVIDF.filter(filterNSVIDF.roadname.isNull())
    unvalidDF = gpsDF.where((gpsDF.longitude != '0.0') | (gpsDF.latitude != '0.0') | (gpsDF.longitude.isNotNull()) | (gpsDF.latitude.isNotNull()) | (gpsDF.longitude != '') | (gpsDF.latitude != '')). \
        withColumn("longitude", format_number(col("longitude").cast("double"), 5)).withColumn("latitude", format_number(col("latitude").cast("double"), 5))

    unvalidDF.createOrReplaceTempView("unvalid_view")
    todoDF = spark.sql("select regexp_extract(unvalid_view.longitude,'([0-9]*.[0-9]{4})') as lgt, regexp_extract(unvalid_view.Latitude,'([0-9]*.[0-9]{4})') as lat from unvalid_view").distinct()
    gpsTodoToday = todoDF.withColumn("gps_point", concat_ws(",", todoDF.lgt, todoDF.lat))

    original = spark.table("data_labeled.gps_data_new")
    originalDF = original.withColumn("gps_point", concat_ws(",", original.lgt, original.lat))

    gpsBaseDF = gpsTodoToday.join(originalDF, on='gps_point', how='left_outer').filter(gpsTodoToday.lgt != '').filter(gpsTodoToday.lat != '')

    gps_list = gpsBaseDF.select("gps_point").distinct().collect()
    gps_array = [str(row.gps_point) for row in gps_list]
    chunks = ['|'.join(gps_array[x:x + 20]) for x in range(0, len(gps_array), 20)]

    # cSchema = StructType([StructField("lgt", StringType()) \
    #     , StructField("lat", StringType()) \
    #     , StructField("province", StringType()) \
    #     , StructField("city", StringType()) \
    #     # , StructField("citycode", StringType()) \
    #     , StructField("roadname", StringType())])

    hivePath = "wasbs://telematics@cmidapn2hotdataprd.blob.core.chinacloudapi.cn/digital_data/data_labeled/gps_data_new/"
    lgt_list, lat_list, province_list, city_list, roadname_list = [], [], [], [], []
    run_times = 0
    for c in range(len(chunks)):
        geocodes(chunks[c], lgt_list, lat_list, province_list, city_list, roadname_list)
        print("finish first {} chunk with 20 gps points".format(c))
        #chunk-> c == 75000 is the number of maximun number of api times
        run_times = c
        if c == 75000-1:
            break

    print("api_used: ", run_times*20)

    df = spark.createDataFrame(zip(lgt_list, lat_list, province_list, city_list, roadname_list),  schema=['lgt', 'lat', 'province', 'city', 'roadname'])
    saveAsTable(df, "")

def geocodes(gps_points, lgt_list=None, lat_list=None, province_list=None, city_list=None, district_list=None, roadname_list=None):
#     poitypes="010000|010100|010101|010102|010103|010104|010105|010107|010108|010109|010110|010111|010112|010200|010300|010400|010401|010500|010600|010700|010800|010900|010901|011000|011100|020000|020100|020101|020102|020103|020104|020105|020106|020200|020201|020202|020203|020300|020301|020400|020401|020402|020403|020404|020405|020406|020407|020408|020600|020601|020602|020700|020701|020702|020703|020800|020900|020904|020905|021000|021001|021002|021003|021004|021100|021200|021201|021202|021203|021300|021301|021400|021401|021500|021501|021600|021601|021602|021700|021701|021702|021800|021802|021803|021804|021900|022000|022100|022200|022300|022301|022400|022500|022501|022502|022600|022700|022800|022900|023000|023100|023200|023300|023301|023400|023500|025000|025100|025200|025300|025400|025500|025600|025700|025800|025900|026000|026100|026200|026300|029900|030000|030100|030200|030201|030202|030203|030204|030205|030206|030300|030301|030302|030303|030400|030401|030500|030501|030502|030503|030504|030505|030506|030507|030508|030700|030701|030702|030800|030801|030802|030803|030900|031000|031004|031005|031100|031101|031102|031103|031104|031200|031300|031301|031302|031303|031400|031401|031500|031501|031600|031601|031700|031701|031702|031800|031801|031802|031900|031902|031903|031904|032000|032100|032200|032300|032400|032401|032500|032600|032601|032602|032700|032800|032900|033000|033100|033200|033300|033400|033401|033500|033600|035000|035100|035200|035300|035400|035500|035600|035700|035800|035900|036000|036100|036200|036300|039900|070400|070401|070500|070501|120100|130600|130601|130602|130603|130604|130605|130606|150300|150304|150900|150903|150904|150905|150906|150907|150908|150909|170300|180200|180201|180202|180203|180300|180301|180302"
    parameters = {'output': 'json', 'location': gps_points, 'key': '4a892e2f733ee31cdda3ff05baf599b7','extensions':'all', 'radius': '500', 'homeorcorp': '2', 'roadlevel':'1', 'batch':'true'}
    base_url = 'https://restapi.amap.com/v3/geocode/regeo'
    response = requests.get(base_url, parameters)
    try:
        print('url:' + response.url)
        answer = response.json()
        #fill gps to list:
        for gps in gps_points.split("|"):
            lgt, lat = gps.split(",")
            lgt_list.append(lgt)
            lat_list.append(lat)
            print(lgt, lat)
        for regeocode in answer['regeocodes']:
            if isinstance(regeocode['addressComponent']['province'], list):
                province_list.append("[]")
            else:
                province_list.append(regeocode['addressComponent']['province'])
            if isinstance(regeocode['addressComponent']['city'], list):
                city_list.append("[]")
            else:
                city_list.append(regeocode['addressComponent']['city'])
            if isinstance(regeocode['roads'], list):
                if len(regeocode['roads']) > 0:
                    roadname_list.append(regeocode['roads'][0]['name'])
            else:
                roadname_list.append("[]")
    except requests.exceptions.ConnectionError:
        response.status_code = "Connection Refused"

def saveAsTable(df, db, tbl, hivePath):
    df.repartition(1).write.format("parquet") \
        .mode("append") \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)



if __name__ == '__main__':
    main()


