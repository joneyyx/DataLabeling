#-*- coding: UTF-8 -*-
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark import *
from pyspark.sql.functions import col, radians, asin, sin, sqrt, cos


spark = SparkSession.builder.appName("data_Label").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")

def main():
    # 20k ESNs first hb data (where these ESNs create date > 20200101)
    # vehicle_detailed_info是更新的表
    build = spark.sql("select ESN, create_time, build_date, ACTUAL_IN_SERVICE_DATE, IN_SERVICE_DATE_AVE_TIME_LAG from de_labeling.vehicle_detailed_info")


    cleanDF = build.withColumn("create_date", to_date(build["create_time"])).filter(col("create_date")>= '2020-01-01')


    for i in range(20200101, 20200132):
        print("report_date = '{}'".format(str(i)))
        hb = spark.sql("select engine_serial_number,occurrence_date_time, engine_speed,gps_vehicle_speed, wheel_based_vehicle_speed ,high_resolution_total_vehicle_distance, latitude,longitude, roadname, province, city, report_date from de_labeling.tel_hb_labeled_sample where report_date = '{}'".format(str(i)))
        joinDF = cleanDF.join(hb, cleanDF.ESN==hb.engine_serial_number, "right").filter(col("ESN").isNotNull())
        saveAsTable(joinDF, "de_labeling", "de_first_run_date", "report_date", hivePath+"de_first_run_date_new2")
        print(">>>>finished: {}".format(str(i)))

#     2. Find the Engine_Speed>0
    for i in range(0, len(partition_list) - 1, 2):
        print("report_date >= '{}' and report_date <= '{}'".format(partition_list[i], partition_list[i + 1]))
        hb = spark.sql("select * from de_labeling.de_first_run_date_new_2 where report_date >= {} and report_date <= {}".format(partition_list[i], partition_list[i + 1])). \
            withColumn("build_date", to_date("build_date")).withColumn("ACTUAL_IN_SERVICE_DATE",to_date("ACTUAL_IN_SERVICE_DATE")).withColumn("IN_SERVICE_DATE_AVE_TIME_LAG", to_date("IN_SERVICE_DATE_AVE_TIME_LAG"))
    # get the mean gps locations ( contains the points with Engine_Speed = 0 )
    # test = hb.groupBy("ESN", "report_date").agg(count("ESN").alias("count_number"), mean("longitude").alias("mean_lng"), mean("latitude").alias("mean_lat"), first("create_date"), first("build_date"), first("ACTUAL_IN_SERVICE_DATE"), first("IN_SERVICE_DATE_AVE_TIME_LAG"), first(""))
        test = hb.groupBy("ESN", "report_date").agg(mean("longitude").alias("mean_lng"), mean("latitude").alias("mean_lat"))
        joinDF = hb.join(test, on=['ESN', 'report_date'], how='left')
        disDF = geodistance(joinDF, "mean_lng", "mean_lat", "longitude", "latitude")
        filterDF = disDF.filter((disDF.engine_speed > 0) & (disDF.haversine_dist > 1000)).groupBy("ESN", "report_date").agg(count("ESN").alias("count_number"), first("build_date").alias("build_date"), first("ACTUAL_IN_SERVICE_DATE").alias("ACTUAL_IN_SERVICE_DATE"),first("IN_SERVICE_DATE_AVE_TIME_LAG").alias("IN_SERVICE_DATE_AVE_TIME_LAG"),first("create_date").alias("create_date"), first("mean_lng").alias("mean_lng"),first("mean_lat").alias("mean_lat"))
        saveAsTable(filterDF, "de_labeling", "de_first_purchase_date_new_2", hivePath + "de_first_purchase_date_new_2/")
        print("saved>>>>")

    #>>>>>>>>>>>>>>>>>2. Find the Engine_Speed>0 | wheel_base_speed>0
    for i in range(0, len(partition_list) - 1, 2):
        print("report_date >= '{}' and report_date <= '{}'".format(partition_list[i], partition_list[i + 1]))
        hb = spark.sql("select * from de_labeling.de_first_run_date_new_2 where report_date >= {} and report_date <= {}".format(partition_list[i], partition_list[i + 1])). \
            withColumn("build_date", to_date("build_date")).withColumn("ACTUAL_IN_SERVICE_DATE", to_date("ACTUAL_IN_SERVICE_DATE")).withColumn("IN_SERVICE_DATE_AVE_TIME_LAG", to_date("IN_SERVICE_DATE_AVE_TIME_LAG"))
        # get the mean gps locations ( contains the points with Engine_Speed = 0 )
        wSpec = Window.partitionBy("ESN", "report_date").orderBy("occurrence_date_time")
        temp = hb.withColumn("mean_lng", mean("longitude").over(wSpec)).withColumn("mean_lat", mean("latitude").over(wSpec)).withColumn("first_lng", first("longitude").over(wSpec)).withColumn("first_lat",first("latitude").over(wSpec))
        disDF = geodistance(temp, "haversine_dist", "mean_lng", "mean_lat", "longitude", "latitude")
        disDF2 = geodistance(disDF, "haversine_dist2", "first_lng", "first_lat", "longitude", "latitude")

        base = disDF.groupBy("ESN", "report_date").agg(first("build_date").alias("build_date"),first("ACTUAL_IN_SERVICE_DATE").alias("ACTUAL_IN_SERVICE_DATE"),
            first("IN_SERVICE_DATE_AVE_TIME_LAG").alias("IN_SERVICE_DATE_AVE_TIME_LAG"), first("create_date").alias("create_date")
            first("mean_lng").alias("mean_lng"), first("mean_lat").alias("mean_lat"),
            (max("high_resolution_total_vehicle_distance") - min("high_resolution_total_vehicle_distance")).alias("mileageDiff"),
            max("haversine_dist").alias("transport_distance"),
            max("haversine_dist2").alias("transport_distance2"))

        #haversine_dist 以平均值为圆心，haversine_dist2以每辆车每天第一个GPS为圆心
        res1 = disDF2.groupBy("ESN", "report_date").agg(
            count(when((disDF2.engine_speed > 0) & (disDF2.haversine_dist > 1000), 1)).alias("count1"), \
            count(when((disDF2.engine_speed > 0) & (disDF2.haversine_dist > 3000), 1)).alias("count2"), \
            count(when((disDF2.engine_speed > 0) & (disDF2.haversine_dist > 5000), 1)).alias("count3"), \
            count(when((disDF2.wheel_based_vehicle_speed > 0) & (disDF2.haversine_dist > 1000), 1)).alias("count4"), \
            count(when((disDF2.wheel_based_vehicle_speed > 0) & (disDF2.haversine_dist > 3000), 1)).alias("count5"), \
            count(when((disDF2.wheel_based_vehicle_speed > 0) & (disDF2.haversine_dist > 5000), 1)).alias("count6"), \
            count(when((disDF2.engine_speed > 0) & (disDF2.haversine_dist2 > 1000), 1)).alias("count7"), \
            count(when((disDF2.engine_speed > 0) & (disDF2.haversine_dist2 > 3000), 1)).alias("count8"), \
            count(when((disDF2.engine_speed > 0) & (disDF2.haversine_dist2 > 5000), 1)).alias("count9"), \
            count(when((disDF2.wheel_based_vehicle_speed > 0) & (disDF2.haversine_dist2 > 1000), 1)).alias("count10"), \
            count(when((disDF2.wheel_based_vehicle_speed > 0) & (disDF2.haversine_dist2 > 3000), 1)).alias("count11"), \
            count(when((disDF2.wheel_based_vehicle_speed > 0) & (disDF2.haversine_dist2 > 5000), 1)).alias("count12"))

        joinDF = base.join(res1, on=['ESN', 'report_date'], how="outer")

        saveAsTable(joinDF, "de_labeling", "de_first_purchase_date_new_3", hivePath + "de_first_purchase_date_new_3/")



partition_list = ['20200101', '20200131', '20200201', '20200229', '20200301', '20200331','20200401', '20200430', '20200501', '20200531']

hivePath="wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/de_labeling/"
def saveAsTable(df, db, tbl, ptkey, hivePath):
    df.write.format("parquet") \
        .mode("append") \
        .option('path', hivePath) \
        .partitionBy(ptkey) \
        .saveAsTable(db + "." + tbl)

def geodistance(df, target_name,lng1, lat1, lng2, lat2):
  result =  df.withColumn("dlon", radians(col(lng1)) - radians(col(lng2))) \
    .withColumn("dlat", radians(col(lat1)) - radians(col(lat2))) \
    .withColumn(target_name, asin(sqrt(sin(col("dlat") / 2) ** 2 + cos(radians(col(lat2)))* cos(radians(col(lat1))) * sin(col("dlon") / 2) ** 2)) * 2 * 3963 * 5280) \
    .drop("dlon", "dlat")
  return result



if __name__ == '__main__':
    main()


def saveAsTable(df, db, tbl, hivePath):
    df.write.format("parquet") \
        .mode("append") \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)